import config from '3lib-config';
import chokidar from 'chokidar';
import fs from 'fs';
import path from 'path';

let sleep = async (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Logging functions
function log(message, ...args) {
  console.log(message, ...args);
}

function logVerbose(message, ...args) {
  if (config.get('verbose', false)) {
    console.log(message, ...args);
  }
}

// HTTP request functions
async function terminateEndpoint(url) {
  try {
    logVerbose(`Sending terminate request to ${url}`);
    const response = await fetch(`http://${url}/api/bentoml/serve/terminate`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      }
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    logVerbose(`Terminate successful for ${url}`, await response.text());
    return true;
  } catch (error) {
    log(`ERROR: Failed to terminate endpoint ${url}:`, error.message);
    return false;
  }
}

async function executeWorkflow(url, port, workflowData) {
  try {
    logVerbose(`Executing workflow on ${url} for port ${port}`);
    const payload = {
      parallel: true,
      host: "0.0.0.0",
      port: port.toString(),
      workflow_api: workflowData
    };

    const response = await fetch(`http://${url}/api/bentoml/serve`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload)
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }

    logVerbose(`Workflow execution successful for ${url}:${port}`, await response.text());
    return true;
  } catch (error) {
    log(`ERROR: Failed to execute workflow on ${url}:${port}:`, error.message);
    return false;
  }
}

// Load workflow JSON file
function loadWorkflowFile(filename) {
  try {
    const filePath = path.resolve(filename);
    const content = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(content);
  } catch (error) {
    log(`ERROR: Failed to load workflow file ${filename}:`, error.message);
    return null;
  }
}

// Main orchestration functions
async function terminateAllEndpoints() {
  const endpoints = config.get('endpoints', []);
  log('Terminating all endpoints...');

  const terminatePromises = endpoints.map(endpoint => terminateEndpoint(endpoint.url));
  await Promise.all(terminatePromises);

  log('All terminations complete');
}

async function executeAllWorkflows() {
  const endpoints = config.get('endpoints', []);
  log('Executing workflows...');

  const workflowPromises = [];

  for (const endpoint of endpoints) {
    for (const workflowConfig of endpoint.workflows || []) {
      const workflowData = loadWorkflowFile(workflowConfig.workflow);
      if (workflowData) {
        // workflowPromises.push(
          await executeWorkflow(endpoint.url, workflowConfig.port, workflowData)
        // );
      }
    }
  }

  await Promise.all(workflowPromises);
  log('All workflows complete');
}

async function executeWorkflowsForEndpoint(endpointUrl) {
  const endpoints = config.get('endpoints', []);
  const endpoint = endpoints.find(ep => ep.url === endpointUrl);

  if (!endpoint) {
    log(`ERROR: Endpoint ${endpointUrl} not found in configuration`);
    return;
  }

  log(`Terminating endpoint ${endpointUrl}...`);
  await terminateEndpoint(endpoint.url);

  log(`Executing workflows for endpoint ${endpointUrl}...`);
  const workflowPromises = [];

  for (const workflowConfig of endpoint.workflows || []) {
    const workflowData = loadWorkflowFile(workflowConfig.workflow);
    if (workflowData) {
      workflowPromises.push(
        executeWorkflow(endpoint.url, workflowConfig.port, workflowData)
      );
    }
  }

  await Promise.all(workflowPromises);
  log(`Workflows complete for endpoint ${endpointUrl}`);
}

// File watching setup
function setupFileWatchers() {
  const endpoints = config.get('endpoints', []);
  const workflowFiles = new Set();
  const fileToEndpoints = new Map();

  // Collect all workflow files and map them to endpoints
  for (const endpoint of endpoints) {
    for (const workflowConfig of endpoint.workflows || []) {
      const filename = workflowConfig.workflow;
      workflowFiles.add(filename);

      if (!fileToEndpoints.has(filename)) {
        fileToEndpoints.set(filename, []);
      }
      fileToEndpoints.get(filename).push(endpoint.url);
    }
  }

  if (workflowFiles.size === 0) {
    logVerbose('No workflow files to watch');
    return null;
  }

  logVerbose(`Setting up watchers for ${workflowFiles.size} workflow files:`, [...workflowFiles]);

  const watcher = chokidar.watch([...workflowFiles], {
    awaitWriteFinish: {
      stabilityThreshold: 1000,
      pollInterval: 100
    },
    persistent: true
  });

  watcher.on('change', async (filename) => {
    log(`Workflow file ${filename} changed, reloading affected endpoints`);
    const affectedEndpoints = fileToEndpoints.get(filename) || [];

    for (const endpointUrl of affectedEndpoints) {
      try {
        await executeWorkflowsForEndpoint(endpointUrl);
      } catch (error) {
        log(`ERROR: Failed to reload endpoint ${endpointUrl}:`, error.message);
      }
    }
  });

  return watcher;
}

// Main orchestration flow
async function runOrchestration() {
  try {
    await terminateAllEndpoints();
    await executeAllWorkflows();
  } catch (error) {
    log('ERROR: Orchestration failed:', error.message);
  }
}

// Configuration change handler
function onConfigChanged() {
  log('Configuration file changed, restarting orchestration');

  // Stop existing file watchers
  if (global.workflowWatcher) {
    global.workflowWatcher.close();
  }

  // Restart orchestration and file watching
  runOrchestration().then(() => {
    global.workflowWatcher = setupFileWatchers();
  }).catch(error => {
    log('ERROR: Failed to restart orchestration:', error.message);
  });
}

// Initialize the system
async function initialize() {
  config.init();

  log('Starting 3suite-comfy-pack orchestrator');

  // Set up config change listener
  config.addListener(onConfigChanged);

  // Run initial orchestration
  await runOrchestration();

  // Set up file watchers
  global.workflowWatcher = setupFileWatchers();

  log('3suite-comfy-pack orchestrator initialized');
}

// Start the application
initialize().catch(error => {
  log('ERROR: Failed to initialize:', error.message);
  process.exit(1);
});

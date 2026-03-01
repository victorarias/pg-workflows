var import_node_module = require("node:module");
var __create = Object.create;
var __getProtoOf = Object.getPrototypeOf;
var __defProp = Object.defineProperty;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __toESM = (mod, isNodeMode, target) => {
  target = mod != null ? __create(__getProtoOf(mod)) : {};
  const to = isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target;
  for (let key of __getOwnPropNames(mod))
    if (!__hasOwnProp.call(to, key))
      __defProp(to, key, {
        get: () => mod[key],
        enumerable: true
      });
  return to;
};
var __moduleCache = /* @__PURE__ */ new WeakMap;
var __toCommonJS = (from) => {
  var entry = __moduleCache.get(from), desc;
  if (entry)
    return entry;
  entry = __defProp({}, "__esModule", { value: true });
  if (from && typeof from === "object" || typeof from === "function")
    __getOwnPropNames(from).map((key) => !__hasOwnProp.call(entry, key) && __defProp(entry, key, {
      get: () => from[key],
      enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable
    }));
  __moduleCache.set(from, entry);
  return entry;
};
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, {
      get: all[name],
      enumerable: true,
      configurable: true,
      set: (newValue) => all[name] = () => newValue
    });
};

// src/index.ts
var exports_src = {};
__export(exports_src, {
  workflow: () => workflow,
  WorkflowStatus: () => WorkflowStatus,
  WorkflowRunNotFoundError: () => WorkflowRunNotFoundError,
  WorkflowEngineError: () => WorkflowEngineError,
  WorkflowEngine: () => WorkflowEngine,
  StepType: () => StepType
});
module.exports = __toCommonJS(exports_src);

// src/definition.ts
function workflow(id, handler, { inputSchema, timeout, retries } = {}) {
  return {
    id,
    handler,
    inputSchema,
    timeout,
    retries
  };
}
// src/engine.ts
var import_es_toolkit = require("es-toolkit");

// src/ast-parser.ts
var ts = __toESM(require("typescript"));
function parseWorkflowHandler(handler) {
  const handlerSource = handler.toString();
  const sourceFile = ts.createSourceFile("handler.ts", handlerSource, ts.ScriptTarget.Latest, true);
  const steps = new Map;
  function isInConditional(node) {
    let current = node.parent;
    while (current) {
      if (ts.isIfStatement(current) || ts.isConditionalExpression(current) || ts.isSwitchStatement(current) || ts.isCaseClause(current)) {
        return true;
      }
      current = current.parent;
    }
    return false;
  }
  function isInLoop(node) {
    let current = node.parent;
    while (current) {
      if (ts.isForStatement(current) || ts.isForInStatement(current) || ts.isForOfStatement(current) || ts.isWhileStatement(current) || ts.isDoStatement(current)) {
        return true;
      }
      current = current.parent;
    }
    return false;
  }
  function extractStepId(arg) {
    if (ts.isStringLiteral(arg) || ts.isNoSubstitutionTemplateLiteral(arg)) {
      return { id: arg.text, isDynamic: false };
    }
    if (ts.isTemplateExpression(arg)) {
      let templateStr = arg.head.text;
      for (const span of arg.templateSpans) {
        templateStr += `\${...}`;
        templateStr += span.literal.text;
      }
      return { id: templateStr, isDynamic: true };
    }
    return { id: arg.getText(sourceFile), isDynamic: true };
  }
  function visit(node) {
    if (ts.isCallExpression(node) && ts.isPropertyAccessExpression(node.expression)) {
      const propertyAccess = node.expression;
      const objectName = propertyAccess.expression.getText(sourceFile);
      const methodName = propertyAccess.name.text;
      if (objectName === "step" && (methodName === "run" || methodName === "waitFor" || methodName === "pause" || methodName === "waitUntil")) {
        const firstArg = node.arguments[0];
        if (firstArg) {
          const { id, isDynamic } = extractStepId(firstArg);
          const stepDefinition = {
            id,
            type: methodName,
            conditional: isInConditional(node),
            loop: isInLoop(node),
            isDynamic
          };
          if (steps.has(id)) {
            throw new Error(`Duplicate step ID detected: '${id}'. Step IDs must be unique within a workflow.`);
          }
          steps.set(id, stepDefinition);
        }
      }
    }
    ts.forEachChild(node, visit);
  }
  visit(sourceFile);
  return { steps: Array.from(steps.values()) };
}

// src/db/migration.ts
async function runMigrations(db) {
  const tableExistsResult = await db.executeSql(`
    SELECT EXISTS (
      SELECT FROM information_schema.tables 
      WHERE table_schema = 'public' 
      AND table_name = 'workflow_runs'
    );
  `, []);
  if (!tableExistsResult.rows[0]?.exists) {
    await db.executeSql(`
      CREATE TABLE workflow_runs (
        id varchar(32) PRIMARY KEY NOT NULL,
        created_at timestamp with time zone DEFAULT now() NOT NULL,
        updated_at timestamp with time zone DEFAULT now() NOT NULL,
        resource_id varchar(32),
        workflow_id varchar(32) NOT NULL,
        status text DEFAULT 'pending' NOT NULL,
        input jsonb NOT NULL,
        output jsonb,
        error text,
        current_step_id varchar(256) NOT NULL,
        timeline jsonb DEFAULT '{}'::jsonb NOT NULL,
        paused_at timestamp with time zone,
        resumed_at timestamp with time zone,
        completed_at timestamp with time zone,
        timeout_at timestamp with time zone,
        retry_count integer DEFAULT 0 NOT NULL,
        max_retries integer DEFAULT 0 NOT NULL,
        job_id varchar(256)
      );
    `, []);
    await db.executeSql(`
      CREATE INDEX workflow_runs_workflow_id_idx ON workflow_runs USING btree (workflow_id);
    `, []);
    await db.executeSql(`
      CREATE INDEX workflow_runs_created_at_idx ON workflow_runs USING btree (created_at);
    `, []);
    await db.executeSql(`
      CREATE INDEX workflow_runs_resource_id_idx ON workflow_runs USING btree (resource_id);
    `, []);
  }
}

// src/db/queries.ts
var import_ksuid = __toESM(require("ksuid"));
function generateKSUID(prefix) {
  return `${prefix ? `${prefix}_` : ""}${import_ksuid.default.randomSync().string}`;
}
function mapRowToWorkflowRun(row) {
  return {
    id: row.id,
    createdAt: new Date(row.created_at),
    updatedAt: new Date(row.updated_at),
    resourceId: row.resource_id,
    workflowId: row.workflow_id,
    status: row.status,
    input: typeof row.input === "string" ? JSON.parse(row.input) : row.input,
    output: typeof row.output === "string" ? row.output.trim().startsWith("{") || row.output.trim().startsWith("[") ? JSON.parse(row.output) : row.output : row.output ?? null,
    error: row.error,
    currentStepId: row.current_step_id,
    timeline: typeof row.timeline === "string" ? JSON.parse(row.timeline) : row.timeline,
    pausedAt: row.paused_at ? new Date(row.paused_at) : null,
    resumedAt: row.resumed_at ? new Date(row.resumed_at) : null,
    completedAt: row.completed_at ? new Date(row.completed_at) : null,
    timeoutAt: row.timeout_at ? new Date(row.timeout_at) : null,
    retryCount: row.retry_count,
    maxRetries: row.max_retries,
    jobId: row.job_id
  };
}
async function insertWorkflowRun({
  resourceId,
  workflowId,
  currentStepId,
  status,
  input,
  maxRetries,
  timeoutAt
}, db) {
  const runId = generateKSUID("run");
  const now = new Date;
  const result = await db.executeSql(`INSERT INTO workflow_runs (
      id, 
      resource_id, 
      workflow_id, 
      current_step_id, 
      status, 
      input, 
      max_retries, 
      timeout_at,
      created_at,
      updated_at,
      timeline,
      retry_count
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
    RETURNING *`, [
    runId,
    resourceId ?? null,
    workflowId,
    currentStepId,
    status,
    JSON.stringify(input),
    maxRetries,
    timeoutAt,
    now,
    now,
    "{}",
    0
  ]);
  const insertedRun = result.rows[0];
  if (!insertedRun) {
    throw new Error("Failed to insert workflow run");
  }
  return mapRowToWorkflowRun(insertedRun);
}
async function getWorkflowRun({
  runId,
  resourceId
}, { exclusiveLock = false, db }) {
  const lockSuffix = exclusiveLock ? "FOR UPDATE" : "";
  const result = resourceId ? await db.executeSql(`SELECT * FROM workflow_runs 
        WHERE id = $1 AND resource_id = $2
        ${lockSuffix}`, [runId, resourceId]) : await db.executeSql(`SELECT * FROM workflow_runs 
        WHERE id = $1
        ${lockSuffix}`, [runId]);
  const run = result.rows[0];
  if (!run) {
    return null;
  }
  return mapRowToWorkflowRun(run);
}
async function updateWorkflowRun({
  runId,
  resourceId,
  data
}, db) {
  const now = new Date;
  const updates = ["updated_at = $1"];
  const values = [now];
  let paramIndex = 2;
  if (data.status !== undefined) {
    updates.push(`status = $${paramIndex}`);
    values.push(data.status);
    paramIndex++;
  }
  if (data.currentStepId !== undefined) {
    updates.push(`current_step_id = $${paramIndex}`);
    values.push(data.currentStepId);
    paramIndex++;
  }
  if (data.timeline !== undefined) {
    updates.push(`timeline = $${paramIndex}`);
    values.push(JSON.stringify(data.timeline));
    paramIndex++;
  }
  if (data.pausedAt !== undefined) {
    updates.push(`paused_at = $${paramIndex}`);
    values.push(data.pausedAt);
    paramIndex++;
  }
  if (data.resumedAt !== undefined) {
    updates.push(`resumed_at = $${paramIndex}`);
    values.push(data.resumedAt);
    paramIndex++;
  }
  if (data.completedAt !== undefined) {
    updates.push(`completed_at = $${paramIndex}`);
    values.push(data.completedAt);
    paramIndex++;
  }
  if (data.output !== undefined) {
    updates.push(`output = $${paramIndex}`);
    values.push(JSON.stringify(data.output));
    paramIndex++;
  }
  if (data.error !== undefined) {
    updates.push(`error = $${paramIndex}`);
    values.push(data.error);
    paramIndex++;
  }
  if (data.retryCount !== undefined) {
    updates.push(`retry_count = $${paramIndex}`);
    values.push(data.retryCount);
    paramIndex++;
  }
  if (data.jobId !== undefined) {
    updates.push(`job_id = $${paramIndex}`);
    values.push(data.jobId);
    paramIndex++;
  }
  const whereClause = resourceId ? `WHERE id = $${paramIndex} AND resource_id = $${paramIndex + 1}` : `WHERE id = $${paramIndex}`;
  values.push(runId);
  if (resourceId) {
    values.push(resourceId);
  }
  const query = `
    UPDATE workflow_runs 
    SET ${updates.join(", ")}
    ${whereClause}
    RETURNING *
  `;
  const result = await db.executeSql(query, values);
  const run = result.rows[0];
  if (!run) {
    return null;
  }
  return mapRowToWorkflowRun(run);
}
async function getWorkflowRuns({
  resourceId,
  startingAfter,
  endingBefore,
  limit = 20,
  statuses,
  workflowId
}, db) {
  const conditions = [];
  const values = [];
  let paramIndex = 1;
  if (resourceId) {
    conditions.push(`resource_id = $${paramIndex}`);
    values.push(resourceId);
    paramIndex++;
  }
  if (statuses && statuses.length > 0) {
    conditions.push(`status = ANY($${paramIndex})`);
    values.push(statuses);
    paramIndex++;
  }
  if (workflowId) {
    conditions.push(`workflow_id = $${paramIndex}`);
    values.push(workflowId);
    paramIndex++;
  }
  if (startingAfter) {
    const cursorResult = await db.executeSql("SELECT created_at FROM workflow_runs WHERE id = $1 LIMIT 1", [startingAfter]);
    if (cursorResult.rows[0]?.created_at) {
      conditions.push(`created_at < $${paramIndex}`);
      values.push(typeof cursorResult.rows[0].created_at === "string" ? new Date(cursorResult.rows[0].created_at) : cursorResult.rows[0].created_at);
      paramIndex++;
    }
  }
  if (endingBefore) {
    const cursorResult = await db.executeSql("SELECT created_at FROM workflow_runs WHERE id = $1 LIMIT 1", [endingBefore]);
    if (cursorResult.rows[0]?.created_at) {
      conditions.push(`created_at > $${paramIndex}`);
      values.push(typeof cursorResult.rows[0].created_at === "string" ? new Date(cursorResult.rows[0].created_at) : cursorResult.rows[0].created_at);
      paramIndex++;
    }
  }
  const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
  const actualLimit = Math.min(Math.max(limit, 1), 100) + 1;
  const query = `
    SELECT * FROM workflow_runs
    ${whereClause}
    ORDER BY created_at DESC
    LIMIT $${paramIndex}
  `;
  values.push(actualLimit);
  const result = await db.executeSql(query, values);
  const rows = result.rows;
  const hasMore = rows.length > (limit ?? 20);
  const rawItems = hasMore ? rows.slice(0, limit) : rows;
  const items = rawItems.map((row) => mapRowToWorkflowRun(row));
  const hasPrev = !!endingBefore;
  const nextCursor = hasMore && items.length > 0 ? items[items.length - 1]?.id ?? null : null;
  const prevCursor = hasPrev && items.length > 0 ? items[0]?.id ?? null : null;
  return { items, nextCursor, prevCursor, hasMore, hasPrev };
}
async function withPostgresTransaction(db, callback) {
  try {
    await db.executeSql("BEGIN", []);
    const result = await callback(db);
    await db.executeSql("COMMIT", []);
    return result;
  } catch (error) {
    await db.executeSql("ROLLBACK", []);
    throw error;
  }
}

// src/error.ts
class WorkflowEngineError extends Error {
  workflowId;
  runId;
  cause;
  constructor(message, workflowId, runId, cause = undefined) {
    super(message);
    this.workflowId = workflowId;
    this.runId = runId;
    this.cause = cause;
    this.name = "WorkflowEngineError";
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, WorkflowEngineError);
    }
  }
}

class WorkflowRunNotFoundError extends WorkflowEngineError {
  constructor(runId, workflowId) {
    super("Workflow run not found", workflowId, runId);
    this.name = "WorkflowRunNotFoundError";
  }
}

// src/types.ts
var WorkflowStatus;
((WorkflowStatus2) => {
  WorkflowStatus2["PENDING"] = "pending";
  WorkflowStatus2["RUNNING"] = "running";
  WorkflowStatus2["PAUSED"] = "paused";
  WorkflowStatus2["COMPLETED"] = "completed";
  WorkflowStatus2["FAILED"] = "failed";
  WorkflowStatus2["CANCELLED"] = "cancelled";
})(WorkflowStatus ||= {});
var StepType;
((StepType2) => {
  StepType2["PAUSE"] = "pause";
  StepType2["RUN"] = "run";
  StepType2["WAIT_FOR"] = "waitFor";
  StepType2["WAIT_UNTIL"] = "waitUntil";
})(StepType ||= {});

// src/engine.ts
var PAUSE_EVENT_NAME = "__internal_pause";
var WORKFLOW_RUN_QUEUE_NAME = "workflow-run";
var LOG_PREFIX = "[WorkflowEngine]";
var StepTypeToIcon = {
  ["run" /* RUN */]: "λ",
  ["waitFor" /* WAIT_FOR */]: "○",
  ["pause" /* PAUSE */]: "⏸",
  ["waitUntil" /* WAIT_UNTIL */]: "⏲"
};
var VALID_TRANSITIONS = {
  ["pending" /* PENDING */]: ["running" /* RUNNING */],
  ["running" /* RUNNING */]: [
    "completed" /* COMPLETED */,
    "failed" /* FAILED */,
    "paused" /* PAUSED */,
    "cancelled" /* CANCELLED */
  ],
  ["paused" /* PAUSED */]: ["running" /* RUNNING */, "cancelled" /* CANCELLED */],
  ["completed" /* COMPLETED */]: [],
  ["failed" /* FAILED */]: [],
  ["cancelled" /* CANCELLED */]: []
};
var defaultLogger = {
  log: (_message) => console.warn(_message),
  error: (message, error) => console.error(message, error)
};
var defaultExpireInSeconds = process.env.WORKFLOW_RUN_EXPIRE_IN_SECONDS ? Number.parseInt(process.env.WORKFLOW_RUN_EXPIRE_IN_SECONDS, 10) : 5 * 60;

class WorkflowEngine {
  boss;
  db;
  unregisteredWorkflows = new Map;
  _started = false;
  workflows = new Map;
  logger;
  constructor({
    workflows,
    logger,
    boss
  } = {}) {
    this.logger = this.buildLogger(logger ?? defaultLogger);
    if (workflows) {
      this.unregisteredWorkflows = new Map(workflows.map((workflow2) => [workflow2.id, workflow2]));
    }
    if (!boss) {
      throw new WorkflowEngineError("PgBoss instance is required in constructor");
    }
    this.boss = boss;
    this.db = boss.getDb();
  }
  async start(asEngine = true, { batchSize } = { batchSize: 1 }) {
    if (this._started) {
      return;
    }
    await this.boss.start();
    await runMigrations(this.boss.getDb());
    if (this.unregisteredWorkflows.size > 0) {
      for (const workflow2 of this.unregisteredWorkflows.values()) {
        await this.registerWorkflow(workflow2);
      }
    }
    await this.boss.createQueue(WORKFLOW_RUN_QUEUE_NAME);
    const numWorkers = +(process.env.WORKFLOW_RUN_WORKERS ?? 3);
    if (asEngine) {
      for (let i = 0;i < numWorkers; i++) {
        await this.boss.work(WORKFLOW_RUN_QUEUE_NAME, { pollingIntervalSeconds: 0.5, batchSize }, (job) => this.handleWorkflowRun(job));
        this.logger.log(`Worker ${i + 1}/${numWorkers} started for queue ${WORKFLOW_RUN_QUEUE_NAME}`);
      }
    }
    this._started = true;
    this.logger.log("Workflow engine started!");
  }
  async stop() {
    await this.boss.stop();
    this._started = false;
    this.logger.log("Workflow engine stopped");
  }
  async registerWorkflow(definition) {
    if (this.workflows.has(definition.id)) {
      throw new WorkflowEngineError(`Workflow ${definition.id} is already registered`, definition.id);
    }
    const { steps } = parseWorkflowHandler(definition.handler);
    this.workflows.set(definition.id, {
      ...definition,
      steps
    });
    this.logger.log(`Registered workflow "${definition.id}" with steps:`);
    for (const step of steps.values()) {
      const tags = [];
      if (step.conditional)
        tags.push("[conditional]");
      if (step.loop)
        tags.push("[loop]");
      if (step.isDynamic)
        tags.push("[dynamic]");
      this.logger.log(`  └─ (${StepTypeToIcon[step.type]}) ${step.id} ${tags.join(" ")}`);
    }
    return this;
  }
  async unregisterWorkflow(workflowId) {
    this.workflows.delete(workflowId);
    return this;
  }
  async unregisterAllWorkflows() {
    this.workflows.clear();
    return this;
  }
  async startWorkflow({
    resourceId,
    workflowId,
    input,
    options
  }) {
    if (!this._started) {
      await this.start(false, { batchSize: options?.batchSize ?? 1 });
    }
    const workflow2 = this.workflows.get(workflowId);
    if (!workflow2) {
      throw new WorkflowEngineError(`Unknown workflow ${workflowId}`);
    }
    if (workflow2.steps.length === 0 || !workflow2.steps[0]) {
      throw new WorkflowEngineError(`Workflow ${workflowId} has no steps`, workflowId);
    }
    if (workflow2.inputSchema) {
      const result = workflow2.inputSchema.safeParse(input);
      if (!result.success) {
        throw new WorkflowEngineError(result.error.message, workflowId);
      }
    }
    const initialStepId = workflow2.steps[0]?.id;
    const run = await withPostgresTransaction(this.boss.getDb(), async (_db) => {
      const timeoutAt = options?.timeout ? new Date(Date.now() + options.timeout) : workflow2.timeout ? new Date(Date.now() + workflow2.timeout) : null;
      const insertedRun = await insertWorkflowRun({
        resourceId,
        workflowId,
        currentStepId: initialStepId,
        status: "running" /* RUNNING */,
        input,
        maxRetries: options?.retries ?? workflow2.retries ?? 0,
        timeoutAt
      }, this.boss.getDb());
      const job = {
        runId: insertedRun.id,
        resourceId,
        workflowId,
        input
      };
      await this.boss.send(WORKFLOW_RUN_QUEUE_NAME, job, {
        startAfter: new Date,
        expireInSeconds: options?.expireInSeconds ?? defaultExpireInSeconds
      });
      return insertedRun;
    });
    this.logger.log("Started workflow run", {
      runId: run.id,
      workflowId
    });
    return run;
  }
  async pauseWorkflow({
    runId,
    resourceId
  }) {
    await this.checkIfHasStarted();
    const run = await this.updateRun({
      runId,
      resourceId,
      data: {
        status: "paused" /* PAUSED */,
        pausedAt: new Date
      }
    });
    this.logger.log("Paused workflow run", {
      runId,
      workflowId: run.workflowId
    });
    return run;
  }
  async resumeWorkflow({
    runId,
    resourceId,
    options
  }) {
    await this.checkIfHasStarted();
    return this.triggerEvent({
      runId,
      resourceId,
      eventName: PAUSE_EVENT_NAME,
      data: {},
      options
    });
  }
  async cancelWorkflow({
    runId,
    resourceId
  }) {
    await this.checkIfHasStarted();
    const run = await this.updateRun({
      runId,
      resourceId,
      data: {
        status: "cancelled" /* CANCELLED */
      }
    });
    this.logger.log(`cancelled workflow run with id ${runId}`);
    return run;
  }
  async triggerEvent({
    runId,
    resourceId,
    eventName,
    data,
    options
  }) {
    await this.checkIfHasStarted();
    const run = await this.getRun({ runId, resourceId });
    const job = {
      runId: run.id,
      resourceId,
      workflowId: run.workflowId,
      input: run.input,
      event: {
        name: eventName,
        data
      }
    };
    this.boss.send(WORKFLOW_RUN_QUEUE_NAME, job, {
      expireInSeconds: options?.expireInSeconds ?? defaultExpireInSeconds
    });
    this.logger.log(`event ${eventName} sent for workflow run with id ${runId}`);
    return run;
  }
  async getRun({ runId, resourceId }, { exclusiveLock = false, db } = {}) {
    const run = await getWorkflowRun({ runId, resourceId }, { exclusiveLock, db: db ?? this.db });
    if (!run) {
      throw new WorkflowRunNotFoundError(runId);
    }
    return run;
  }
  async updateRun({
    runId,
    resourceId,
    data
  }, { db } = {}) {
    if (data.status !== undefined) {
      const current = await this.getRun({ runId, resourceId }, { db });
      this.validateTransition(runId, current.status, data.status);
    }
    const run = await updateWorkflowRun({ runId, resourceId, data }, db ?? this.db);
    if (!run) {
      throw new WorkflowRunNotFoundError(runId);
    }
    return run;
  }
  validateTransition(runId, from, to) {
    if (from === to)
      return;
    const allowed = VALID_TRANSITIONS[from];
    if (!allowed || !allowed.includes(to)) {
      throw new WorkflowEngineError(`Invalid status transition from "${from}" to "${to}"`, undefined, runId);
    }
  }
  async checkProgress({
    runId,
    resourceId
  }) {
    const run = await this.getRun({ runId, resourceId });
    const workflow2 = this.workflows.get(run.workflowId);
    if (!workflow2) {
      throw new WorkflowEngineError(`Workflow ${run.workflowId} not found`, run.workflowId, runId);
    }
    const steps = workflow2?.steps ?? [];
    let completionPercentage = 0;
    let completedSteps = 0;
    if (steps.length > 0) {
      completedSteps = Object.values(run.timeline).filter((step) => typeof step === "object" && step !== null && ("output" in step) && step.output !== undefined).length;
      if (run.status === "completed" /* COMPLETED */) {
        completionPercentage = 100;
      } else if (run.status === "failed" /* FAILED */ || run.status === "cancelled" /* CANCELLED */) {
        completionPercentage = Math.min(completedSteps / steps.length * 100, 100);
      } else {
        const currentStepIndex = steps.findIndex((step) => step.id === run.currentStepId);
        if (currentStepIndex >= 0) {
          completionPercentage = currentStepIndex / steps.length * 100;
        } else {
          const completedSteps2 = Object.keys(run.timeline).length;
          completionPercentage = Math.min(completedSteps2 / steps.length * 100, 100);
        }
      }
    }
    return {
      ...run,
      completedSteps,
      completionPercentage: Math.round(completionPercentage * 100) / 100,
      totalSteps: steps.length
    };
  }
  async handleWorkflowRun([job]) {
    const { runId, resourceId, workflowId, input, event } = job?.data ?? {};
    if (!runId) {
      throw new WorkflowEngineError("Invalid workflow run job, missing runId", workflowId);
    }
    if (!resourceId) {
      throw new WorkflowEngineError("Invalid workflow run job, missing resourceId", workflowId);
    }
    if (!workflowId) {
      throw new WorkflowEngineError("Invalid workflow run job, missing workflowId", undefined, runId);
    }
    const workflow2 = this.workflows.get(workflowId);
    if (!workflow2) {
      throw new WorkflowEngineError(`Workflow ${workflowId} not found`, workflowId, runId);
    }
    this.logger.log("Processing workflow run...", {
      runId,
      workflowId
    });
    let run = await this.getRun({ runId, resourceId });
    try {
      if (run.status === "cancelled" /* CANCELLED */) {
        this.logger.log(`Workflow run ${runId} is cancelled, skipping`);
        return;
      }
      if (!run.currentStepId) {
        throw new WorkflowEngineError("Missing current step id", workflowId, runId);
      }
      if (run.status === "paused" /* PAUSED */) {
        const waitForStepEntry = run.timeline[`${run.currentStepId}-wait-for`];
        const waitForStep = waitForStepEntry && typeof waitForStepEntry === "object" && "waitFor" in waitForStepEntry ? waitForStepEntry : null;
        const currentStepEntry = run.timeline[run.currentStepId];
        const currentStep = currentStepEntry && typeof currentStepEntry === "object" && "output" in currentStepEntry ? currentStepEntry : null;
        const waitFor = waitForStep?.waitFor;
        const hasCurrentStepOutput = currentStep?.output !== undefined;
        if (waitFor && waitFor.eventName === event?.name && !hasCurrentStepOutput) {
          run = await this.updateRun({
            runId,
            resourceId,
            data: {
              status: "running" /* RUNNING */,
              pausedAt: null,
              resumedAt: new Date,
              timeline: import_es_toolkit.merge(run.timeline, {
                [run.currentStepId]: {
                  output: event?.data ?? {},
                  completedAt: new Date
                }
              }),
              jobId: job?.id
            }
          });
        } else {
          run = await this.updateRun({
            runId,
            resourceId,
            data: {
              status: "running" /* RUNNING */,
              pausedAt: null,
              resumedAt: new Date,
              jobId: job?.id
            }
          });
        }
      }
      const context = {
        input: run.input,
        workflowId: run.workflowId,
        runId: run.id,
        timeline: run.timeline,
        logger: this.logger,
        step: {
          run: async (stepId, handler) => {
            if (!run) {
              throw new WorkflowEngineError("Missing workflow run", workflowId, runId);
            }
            return this.runStep({
              stepId,
              run,
              handler
            });
          },
          waitFor: async (stepId, { eventName, timeout }) => {
            if (!run) {
              throw new WorkflowEngineError("Missing workflow run", workflowId, runId);
            }
            return this.waitForEvent({
              run,
              stepId,
              eventName,
              timeout
            });
          },
          waitUntil: async (stepId, { date }) => {
            if (!run) {
              throw new WorkflowEngineError("Missing workflow run", workflowId, runId);
            }
            await this.waitUntilDate({
              run,
              stepId,
              date
            });
          },
          pause: async (stepId) => {
            if (!run) {
              throw new WorkflowEngineError("Missing workflow run", workflowId, runId);
            }
            return this.pauseStep({
              stepId,
              run
            });
          }
        }
      };
      const result = await workflow2.handler(context);
      run = await this.getRun({ runId, resourceId });
      if (run.status === "running" /* RUNNING */ && run.currentStepId === workflow2.steps[workflow2.steps.length - 1]?.id) {
        const normalizedResult = result === undefined ? {} : result;
        await this.updateRun({
          runId,
          resourceId,
          data: {
            status: "completed" /* COMPLETED */,
            output: normalizedResult,
            completedAt: new Date,
            jobId: job?.id
          }
        });
        this.logger.log("Workflow run completed.", {
          runId,
          workflowId
        });
      }
    } catch (error) {
      try {
        run = await this.getRun({ runId, resourceId });
      } catch {}
      const stepError = error instanceof Error ? error.message : String(error);
      const timelineWithError = run.currentStepId ? import_es_toolkit.merge(run.timeline, { [run.currentStepId]: { error: stepError } }) : run.timeline;
      if (run.retryCount < run.maxRetries) {
        await this.updateRun({
          runId,
          resourceId,
          data: {
            status: "running" /* RUNNING */,
            error: null,
            retryCount: run.retryCount + 1,
            timeline: timelineWithError,
            jobId: job?.id
          }
        });
        const retryDelay = 2 ** run.retryCount * 1000;
        const pgBossJob = {
          runId,
          resourceId,
          workflowId,
          input
        };
        await this.boss?.send("workflow-run", pgBossJob, {
          startAfter: new Date(Date.now() + retryDelay),
          expireInSeconds: defaultExpireInSeconds
        });
        return;
      }
      await this.updateRun({
        runId,
        resourceId,
        data: {
          status: "failed" /* FAILED */,
          error: stepError,
          timeline: timelineWithError,
          jobId: job?.id
        }
      });
      throw error;
    }
  }
  async runStep({
    stepId,
    run,
    handler
  }) {
    const latestRun = await this.getRun({ runId: run.id, resourceId: run.resourceId ?? undefined });
    if (latestRun.status === "cancelled" /* CANCELLED */ || latestRun.status === "paused" /* PAUSED */ || latestRun.status === "failed" /* FAILED */) {
      return;
    }
    const existingEntry = latestRun.timeline[stepId];
    const alreadyCompleted = existingEntry && typeof existingEntry === "object" && "output" in existingEntry && existingEntry.output !== undefined;
    if (!alreadyCompleted) {
      run = await this.updateRun({
        runId: run.id,
        resourceId: run.resourceId ?? undefined,
        data: {
          currentStepId: stepId,
          timeline: import_es_toolkit.merge(latestRun.timeline, {
            [stepId]: { startedAt: new Date }
          })
        }
      });
    }
    return withPostgresTransaction(this.db, async (db) => {
      const persistedRun = await this.getRun({ runId: run.id, resourceId: run.resourceId ?? undefined }, {
        exclusiveLock: true,
        db
      });
      if (persistedRun.status === "cancelled" /* CANCELLED */ || persistedRun.status === "paused" /* PAUSED */ || persistedRun.status === "failed" /* FAILED */) {
        this.logger.log(`Step ${stepId} skipped, workflow run is ${persistedRun.status}`, {
          runId: run.id,
          workflowId: run.workflowId
        });
        return;
      }
      try {
        let result;
        const timelineStepEntry = persistedRun.timeline[stepId];
        const timelineStep = timelineStepEntry && typeof timelineStepEntry === "object" && "output" in timelineStepEntry ? timelineStepEntry : null;
        if (timelineStep?.output !== undefined) {
          result = timelineStep.output;
        } else {
          this.logger.log(`Running step ${stepId}...`, {
            runId: run.id,
            workflowId: run.workflowId
          });
          result = await handler();
          run = await this.updateRun({
            runId: run.id,
            resourceId: run.resourceId ?? undefined,
            data: {
              timeline: import_es_toolkit.merge(persistedRun.timeline, {
                [stepId]: {
                  output: result === undefined ? {} : result,
                  completedAt: new Date
                }
              })
            }
          }, { db });
        }
        const finalResult = result === undefined ? {} : result;
        return finalResult;
      } catch (error) {
        this.logger.error(`Step ${stepId} failed:`, error, {
          runId: run.id,
          workflowId: run.workflowId
        });
        throw error;
      }
    });
  }
  async waitForEvent({
    run,
    stepId,
    eventName,
    timeout
  }) {
    const persistedRun = await this.getRun({
      runId: run.id,
      resourceId: run.resourceId ?? undefined
    });
    if (persistedRun.status === "cancelled" /* CANCELLED */ || persistedRun.status === "paused" /* PAUSED */ || persistedRun.status === "failed" /* FAILED */) {
      this.logger.log(`Step ${stepId} skipped, workflow run is ${persistedRun.status}`, {
        runId: run.id,
        workflowId: run.workflowId
      });
      return;
    }
    const timelineStepCheckEntry = persistedRun.timeline[stepId];
    const timelineStepCheck = timelineStepCheckEntry && typeof timelineStepCheckEntry === "object" && "output" in timelineStepCheckEntry ? timelineStepCheckEntry : null;
    if (timelineStepCheck?.output !== undefined) {
      return timelineStepCheck.output;
    }
    await this.updateRun({
      runId: run.id,
      resourceId: run.resourceId ?? undefined,
      data: {
        status: "paused" /* PAUSED */,
        currentStepId: stepId,
        timeline: import_es_toolkit.merge(persistedRun.timeline, {
          [`${stepId}-wait-for`]: {
            waitFor: {
              eventName,
              timeout
            },
            completedAt: new Date
          }
        }),
        pausedAt: new Date
      }
    });
    this.logger.log(`Running step ${stepId}, waiting for event ${eventName}...`, {
      runId: run.id,
      workflowId: run.workflowId
    });
  }
  async pauseStep({ stepId, run }) {
    await this.waitForEvent({
      run,
      stepId,
      eventName: PAUSE_EVENT_NAME
    });
  }
  async waitUntilDate({
    run,
    stepId,
    date
  }) {
    const eventName = `__wait_until_${stepId}`;
    await this.waitForEvent({
      run,
      stepId,
      eventName
    });
    const job = {
      runId: run.id,
      resourceId: run.resourceId ?? undefined,
      workflowId: run.workflowId,
      input: run.input,
      event: {
        name: eventName,
        data: { date: date.toISOString() }
      }
    };
    await this.boss.send(WORKFLOW_RUN_QUEUE_NAME, job, {
      startAfter: date,
      expireInSeconds: defaultExpireInSeconds
    });
    this.logger.log(`Running step ${stepId}, waiting until ${date.toISOString()}...`, {
      runId: run.id,
      workflowId: run.workflowId
    });
  }
  async checkIfHasStarted() {
    if (!this._started) {
      throw new WorkflowEngineError("Workflow engine not started");
    }
  }
  buildLogger(logger) {
    return {
      log: (message, context) => {
        const { runId, workflowId } = context ?? {};
        const parts = [LOG_PREFIX, workflowId, runId].filter(Boolean).join(" ");
        logger.log(`${parts}: ${message}`);
      },
      error: (message, error, context) => {
        const { runId, workflowId } = context ?? {};
        const parts = [LOG_PREFIX, workflowId, runId].filter(Boolean).join(" ");
        logger.error(`${parts}: ${message}`, error);
      }
    };
  }
  async getRuns({
    resourceId,
    startingAfter,
    endingBefore,
    limit = 20,
    statuses,
    workflowId
  }) {
    return getWorkflowRuns({
      resourceId,
      startingAfter,
      endingBefore,
      limit,
      statuses,
      workflowId
    }, this.db);
  }
}

//# debugId=97ACEAE749D1D39764756E2164756E21
//# sourceMappingURL=index.js.map

import { z } from "zod";
type WorkflowRun = {
	id: string;
	createdAt: Date;
	updatedAt: Date;
	resourceId: string | null;
	workflowId: string;
	status: "pending" | "running" | "paused" | "completed" | "failed" | "cancelled";
	input: unknown;
	output: unknown | null;
	error: string | null;
	currentStepId: string;
	timeline: Record<string, unknown>;
	pausedAt: Date | null;
	resumedAt: Date | null;
	completedAt: Date | null;
	timeoutAt: Date | null;
	retryCount: number;
	maxRetries: number;
	jobId: string | null;
};
declare enum WorkflowStatus {
	PENDING = "pending",
	RUNNING = "running",
	PAUSED = "paused",
	COMPLETED = "completed",
	FAILED = "failed",
	CANCELLED = "cancelled"
}
declare enum StepType {
	PAUSE = "pause",
	RUN = "run",
	WAIT_FOR = "waitFor",
	WAIT_UNTIL = "waitUntil"
}
type Parameters = z.ZodTypeAny;
type inferParameters<P extends Parameters> = P extends z.ZodTypeAny ? z.infer<P> : never;
type WorkflowOptions<I extends Parameters> = {
	timeout?: number;
	retries?: number;
	inputSchema?: I;
};
interface WorkflowLogger {
	log(message: string): void;
	error(message: string, ...args: unknown[]): void;
}
type InternalWorkflowLoggerContext = {
	runId?: string;
	workflowId?: string;
};
interface InternalWorkflowLogger {
	log(message: string, context?: InternalWorkflowLoggerContext): void;
	error(message: string, error: Error, context?: InternalWorkflowLoggerContext): void;
}
type StepContext = {
	run: <T>(stepId: string, handler: () => Promise<T>) => Promise<T>;
	waitFor: <T extends Parameters>(stepId: string, { eventName, timeout, schema }: {
		eventName: string;
		timeout?: number;
		schema?: T;
	}) => Promise<inferParameters<T>>;
	waitUntil: (stepId: string, { date }: {
		date: Date;
	}) => Promise<void>;
	pause: (stepId: string) => Promise<void>;
};
type WorkflowContext<T extends Parameters = Parameters> = {
	input: T;
	step: StepContext;
	workflowId: string;
	runId: string;
	timeline: Record<string, unknown>;
	logger: WorkflowLogger;
};
type WorkflowDefinition<T extends Parameters = Parameters> = {
	id: string;
	handler: (context: WorkflowContext<inferParameters<T>>) => Promise<unknown>;
	inputSchema?: T;
	timeout?: number;
	retries?: number;
};
type InternalStepDefinition = {
	id: string;
	type: StepType;
	conditional: boolean;
	loop: boolean;
	isDynamic: boolean;
};
type InternalWorkflowDefinition<T extends Parameters = Parameters> = WorkflowDefinition<T> & {
	steps: InternalStepDefinition[];
};
type WorkflowRunProgress = WorkflowRun & {
	completionPercentage: number;
	totalSteps: number;
	completedSteps: number;
};
declare function workflow<I extends Parameters>(id: string, handler: (context: WorkflowContext<inferParameters<I>>) => Promise<unknown>, { inputSchema, timeout, retries }?: WorkflowOptions<I>): WorkflowDefinition<I>;
import { Db, PgBoss } from "pg-boss";
declare class WorkflowEngine {
	private boss;
	private db;
	private unregisteredWorkflows;
	private _started;
	workflows: Map<string, InternalWorkflowDefinition>;
	private logger;
	constructor({ workflows, logger, boss }?: Partial<{
		workflows: WorkflowDefinition[];
		logger: WorkflowLogger;
		boss: PgBoss;
	}>);
	start(asEngine?: boolean, { batchSize }?: {
		batchSize?: number;
	}): Promise<void>;
	stop(): Promise<void>;
	registerWorkflow(definition: WorkflowDefinition): Promise<WorkflowEngine>;
	unregisterWorkflow(workflowId: string): Promise<WorkflowEngine>;
	unregisterAllWorkflows(): Promise<WorkflowEngine>;
	startWorkflow({ resourceId, workflowId, input, options }: {
		resourceId?: string;
		workflowId: string;
		input: unknown;
		options?: {
			timeout?: number;
			retries?: number;
			expireInSeconds?: number;
			batchSize?: number;
		};
	}): Promise<WorkflowRun>;
	pauseWorkflow({ runId, resourceId }: {
		runId: string;
		resourceId?: string;
	}): Promise<WorkflowRun>;
	resumeWorkflow({ runId, resourceId, options }: {
		runId: string;
		resourceId?: string;
		options?: {
			expireInSeconds?: number;
		};
	}): Promise<WorkflowRun>;
	cancelWorkflow({ runId, resourceId }: {
		runId: string;
		resourceId?: string;
	}): Promise<WorkflowRun>;
	triggerEvent({ runId, resourceId, eventName, data, options }: {
		runId: string;
		resourceId?: string;
		eventName: string;
		data?: Record<string, unknown>;
		options?: {
			expireInSeconds?: number;
		};
	}): Promise<WorkflowRun>;
	getRun({ runId, resourceId }: {
		runId: string;
		resourceId?: string;
	}, { exclusiveLock, db }?: {
		exclusiveLock?: boolean;
		db?: Db;
	}): Promise<WorkflowRun>;
	updateRun({ runId, resourceId, data }: {
		runId: string;
		resourceId?: string;
		data: Partial<WorkflowRun>;
	}, { db }?: {
		db?: Db;
	}): Promise<WorkflowRun>;
	private validateTransition;
	checkProgress({ runId, resourceId }: {
		runId: string;
		resourceId?: string;
	}): Promise<WorkflowRunProgress>;
	private handleWorkflowRun;
	private runStep;
	private waitForEvent;
	private pauseStep;
	private waitUntilDate;
	private checkIfHasStarted;
	private buildLogger;
	getRuns({ resourceId, startingAfter, endingBefore, limit, statuses, workflowId }: {
		resourceId?: string;
		startingAfter?: string | null;
		endingBefore?: string | null;
		limit?: number;
		statuses?: WorkflowStatus[];
		workflowId?: string;
	}): Promise<{
		items: WorkflowRun[];
		nextCursor: string | null;
		prevCursor: string | null;
		hasMore: boolean;
		hasPrev: boolean;
	}>;
}
declare class WorkflowEngineError extends Error {
	readonly workflowId?: string | undefined;
	readonly runId?: string | undefined;
	readonly cause: Error | undefined;
	constructor(message: string, workflowId?: string | undefined, runId?: string | undefined, cause?: Error | undefined);
}
declare class WorkflowRunNotFoundError extends WorkflowEngineError {
	constructor(runId?: string, workflowId?: string);
}
export { workflow, inferParameters, WorkflowStatus, WorkflowRunProgress, WorkflowRunNotFoundError, WorkflowOptions, WorkflowLogger, WorkflowEngineError, WorkflowEngine, WorkflowDefinition, WorkflowContext, StepType, StepContext, Parameters, InternalWorkflowLoggerContext, InternalWorkflowLogger, InternalWorkflowDefinition, InternalStepDefinition };

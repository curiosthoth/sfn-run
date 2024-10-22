package main

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/aws"
	cwlogstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	cwlogs "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
)

type StepOutput struct {
	StopCode      string
	StoppedReason string
	TaskArn       *string
}

type CheckStatus int

const (
	Ok            CheckStatus = 0
	Failed        CheckStatus = 1
	TimeOut       CheckStatus = 2
	TooManyErrors CheckStatus = 3
	Aborted       CheckStatus = 4
)

const (
	MaxTimeoutSeconds = 7200
)

func checkAndWaitForRunningExecution(svc *sfn.Client, stateMachineArn *string,
	secondsToWait int, pollInterval int) CheckStatus {
	var i = 0
	var errCount = 0
	for ; i < secondsToWait; i += pollInterval {
		output, err := svc.ListExecutions(context.Background(), &sfn.ListExecutionsInput{
			StateMachineArn: stateMachineArn,
			StatusFilter:    types.ExecutionStatusRunning,
		})
		if err != nil {
			log.Errorf("Error while listing executions. %v", err)
			errCount += 1
		}
		if errCount >= 3 {
			return TooManyErrors
		}
		if output == nil {
			continue
		}
		if len(output.Executions) == 0 {
			return Ok
		}
		arns := make([]string, 1)
		for _, execution := range output.Executions {
			arn := execution.ExecutionArn
			arns = append(arns, *arn)
		}
		log.Infof("There are other executions currently running. Arns:\n%s\n", strings.Join(arns, "\n"))
		time.Sleep(time.Duration(pollInterval) * time.Second)
	}
	if i >= secondsToWait {
		return TimeOut
	}
	return Ok
}

func startStateMachineSync(svc *sfn.Client, stateMachineArn *string) string {
	output, err := svc.StartExecution(context.TODO(), &sfn.StartExecutionInput{
		StateMachineArn: stateMachineArn,
	})
	if err != nil {
		log.Fatalf("%v\n", err)
	}
	return *output.ExecutionArn
}

func waitForExecution(svc *sfn.Client, executionArn string, secondsToWait int, pollInterval int) (CheckStatus, *StepOutput) {
	var i = 0
	var errCount = 0
	var stepOutput *StepOutput = nil
	for ; i < secondsToWait; i += pollInterval {
		result, err := svc.DescribeExecution(context.TODO(), &sfn.DescribeExecutionInput{
			ExecutionArn: &executionArn,
		})
		if err != nil {
			errCount += 1
			continue
		}
		if errCount >= 3 {
			return TooManyErrors, stepOutput
		}

		status := result.Status
		log.Infof("Status: %s\n", status)
		if status == types.ExecutionStatusRunning {
			time.Sleep(time.Duration(pollInterval) * time.Second)
			continue
		}
		// The run stopped at this point, we need to figure out if
		// there are any issues.
		if status == types.ExecutionStatusSucceeded {
			// Finds the first succeeded task
			result, err := svc.GetExecutionHistory(
				context.TODO(),
				&sfn.GetExecutionHistoryInput{
					ExecutionArn: &executionArn,
				},
			)
			if err != nil {
				log.Errorf("Error while getting execution history. %s", err)
			}
			if result != nil {
				for _, event := range result.Events {
					if event.Type == types.HistoryEventTypeTaskSucceeded && event.TaskSucceededEventDetails != nil {
						outputStr := event.TaskSucceededEventDetails.Output
						if outputStr != nil {
							stepOutput = parseStateMachineStepOutput(*outputStr)
						}
						break
					}
				}
			}

			return Ok, stepOutput
		}
		if status == types.ExecutionStatusFailed {
			// Finds the first failed task
			result, err := svc.GetExecutionHistory(
				context.TODO(),
				&sfn.GetExecutionHistoryInput{
					ExecutionArn: &executionArn,
				},
			)
			if err != nil {
				log.Errorf("Error while getting execution history. %s", err)
			}
			if result != nil {
				for _, event := range result.Events {
					if event.Type == types.HistoryEventTypeTaskFailed && event.TaskFailedEventDetails != nil {
						outputStr := event.TaskFailedEventDetails.Cause
						if outputStr != nil {
							stepOutput = parseStateMachineStepOutput(*outputStr)
						}
						break
					}
				}
			}
			return Failed, stepOutput
		}
		return Aborted, stepOutput
	}
	if i >= secondsToWait {
		// Stop the execution
		log.Warnf("Stopping execution %s due to timeout (%d)", executionArn, secondsToWait)
		_, err := svc.StopExecution(context.TODO(), &sfn.StopExecutionInput{
			ExecutionArn: &executionArn,
			Cause:        aws.String("Timeout"),
		})
		if err != nil {
			log.Errorf("Error while stopping execution. %s", err)
		}
		return TimeOut, stepOutput
	}
	return Ok, stepOutput
}

func getLogs(svc *cwlogs.Client, logGroupName string, logStreamName string) *string {
	var limit int32 = 100
	result, err := svc.GetLogEvents(context.TODO(), &cwlogs.GetLogEventsInput{
		LogGroupName:  &logGroupName,
		LogStreamName: &logStreamName,
		Limit:         &limit,
	})
	if err != nil {
		log.Errorf("Error retrieving log events from logGroup: %s; logStream: %s", logGroupName, logStreamName)
		return nil
	}
	ret := make([]string, 0)
	for _, event := range result.Events {
		if event.Message != nil {
			ret = append(ret, *event.Message)
		}
	}
	retStr := strings.Join(ret, "\n")
	return &retStr
}

// parseStateMachineStepOutput returns the Task Arn from an output or cause from
// a StateMachine step output.
func parseStateMachineStepOutput(output string) *StepOutput {
	var result map[string]interface{}
	err := json.Unmarshal([]byte(output), &result)
	if err != nil {
		return nil
	}
	ret := StepOutput{}
	if v, exists := result["StopCode"]; exists {
		ret.StopCode = v.(string)
	}
	if v, exists := result["StoppedReason"]; exists {
		ret.StoppedReason = v.(string)
	}
	if v, exists := result["TaskArn"]; exists && ret.StopCode == "EssentialContainerExited" {
		// We only populate the TaskArn if the container has been launched.
		// otherwise there is no point for fetching the logs.
		taskArn := v.(string)
		ret.TaskArn = &taskArn
	}

	return &ret
}

// findLogStream returns the first LogStream Name that matches the given filter (substring match)
func findLogStream(svc *cwlogs.Client, logGroupName string, filter string) *string {
	descending := true
	var limit int32 = 50
	result, err := svc.DescribeLogStreams(context.TODO(), &cwlogs.DescribeLogStreamsInput{
		LogGroupName: &logGroupName,
		OrderBy:      cwlogstypes.OrderByLastEventTime,
		Descending:   &descending,
		Limit:        &limit,
	})
	if err != nil {
		log.Printf("Error while describing streams for %s. %s", logGroupName, err)
		return nil
	}
	for _, logStream := range result.LogStreams {
		name := logStream.LogStreamName
		if name != nil && strings.Index(*name, filter) > 0 {
			return logStream.LogStreamName
		}
	}
	return nil
}

func main() {
	formatter := log.TextFormatter{
		TimestampFormat: time.RFC3339,
		FullTimestamp:   true,
	}

	log.SetFormatter(&formatter)

	// Using the SDK's default configuration, loading additional config
	// and credentials values from the environment variables, shared
	// credentials, and shared configuration files
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	args := os.Args
	if len(args) != 5 {
		log.Fatalf("Wrong number of arguments. Should be: \n" +
			"./sfn-run <state-machine-arn> <log-group-name> <timeout-in-seconds> <allow-multiple>")
	}

	stateMachineArn := strings.TrimSpace(args[1])
	logGroupName := strings.TrimSpace(args[2])
	timeoutSeconds, err := strconv.Atoi(args[3])
	if err != nil {
		log.Fatalf("Invalid timeout value: %s", args[3])
	}
	if timeoutSeconds > MaxTimeoutSeconds {
		log.Fatalf("Timeout value is too high. Max allowed is %d", MaxTimeoutSeconds)
	}
	if timeoutSeconds < 15 {
		log.Fatalf("Timeout value must be equal or greater than 15 seconds")
	}
	allowMulti := strings.ToLower(strings.TrimSpace(args[4]))

	svc := sfn.NewFromConfig(cfg)

	// We will skip check if the `allowMulti` is `true`.
	if allowMulti != "true" {
		log.Infof("Waiting for other executions to finish (%d seconds max)\n", MaxTimeoutSeconds)
		status := checkAndWaitForRunningExecution(
			svc,
			&stateMachineArn,
			MaxTimeoutSeconds,
			15,
		)
		if status == TimeOut {
			log.Fatal("Wait for other executions timed out. Stopping.")
		}
		if status == TooManyErrors {
			log.Fatal("There were too many errors when waiting for other executions. Stopping.")
		}
		log.Infof("Good! No other executions are running.")
	} else {
		log.Infof("We are not checking for existing executions. Make sure the job is reentrant.")
	}

	executionArn := startStateMachineSync(svc, &stateMachineArn)
	log.Infof("State Machine %s started. Timeout= %d seconds", stateMachineArn, timeoutSeconds)
	status, stepOutput := waitForExecution(svc, executionArn, timeoutSeconds, 15)
	if stepOutput != nil {
		// We should have logs if this
		// split off the last segment after `/`
		taskArn := stepOutput.TaskArn
		log.Infof("StopCode:%s\nStoppedReason:%s\n", stepOutput.StopCode, stepOutput.StoppedReason)
		if taskArn != nil {
			splitted := strings.Split(*taskArn, "/")
			var substrToMatch string
			if len(splitted) > 0 {
				substrToMatch = splitted[len(splitted)-1]
			} else {
				substrToMatch = *taskArn
			}

			logsSvc := cwlogs.NewFromConfig(cfg)
			logStreamName := findLogStream(logsSvc, logGroupName, substrToMatch)
			if logStreamName != nil {
				logStr := getLogs(logsSvc, logGroupName, *logStreamName)
				if logStr != nil {
					println(*logStr)
				}
			}
		}
	}
	if status == Failed || status == TooManyErrors || status == TimeOut {
		os.Exit(1)
	}
}

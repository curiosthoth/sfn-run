sfn-run
====

This is a handy tool that triggers an AWS StepFunctions state machine, waits for it to finish, and returns the output,
along with the log (if logGroup name is given).

Usage

```shell
./sfn-run <state-machine-arn> <log-group-name> <timeout-in-seconds> <allow-multiple>
```

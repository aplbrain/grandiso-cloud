# GrandIsoCloud

## Usage

```
NAME
    grandisocloud.py

SYNOPSIS
    grandisocloud.py COMMAND

COMMANDS
    COMMAND is one of the following:

     init
       Initialize a new GrandIsoCloud job.

     run
       Run a GrandIsoCloud job.
```

## `init`

```
NAME
    grandisocloud.py init - Initialize a new GrandIsoCloud job.

SYNOPSIS
    grandisocloud.py init JOB_ID QUEUE_URI HOST_GRAND_URI

DESCRIPTION
    Initialize a new GrandIsoCloud job.

POSITIONAL ARGUMENTS
    JOB_ID
        Type: str
        The job ID to create. Will not be checked for uniqueness.
    QUEUE_URI
        Type: str
        The URI of the queue to use. See ptq for more details.
    HOST_GRAND_URI
        Type: str
        The URI of the host graph to use (see grand docs for more details).

NOTES
    You can also use flags syntax for POSITIONAL ARGUMENTS
```

## `run`

```
NAME
    grandisocloud.py run - Run a GrandIsoCloud job.

SYNOPSIS
    grandisocloud.py run JOB_ID QUEUE_URI <flags>

DESCRIPTION
    Attaches to an existing work queue. ALL jobs in the queue will be processed
    by this worker, so if you have multiple jobs with different host graphs,
    make sure they are all available and visible to each worker. (Otherwise,
    use different queue URIs for each host graph.)

    If you have multiple workers, you can use the same queue URI for each.

POSITIONAL ARGUMENTS
    JOB_ID
        Type: str
        The job ID to run.
    QUEUE_URI
        Type: str
        The URI of the queue to use. See ptq for more details.

FLAGS
    --verbose=VERBOSE
        Type: bool
        Default: False
        Whether to print progress information.
    --lease_seconds=LEASE_SECONDS
        Type: Optional[int]
        Default: None
        The number of seconds to wait for a job before timing out.
    --tally=TALLY
        Type: bool
        Default: False
        Whether to keep a tally of the number of jobs processed.

NOTES
    You can also use flags syntax for POSITIONAL ARGUMENTS
```

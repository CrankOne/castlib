
# Rationale

The CASTOR storage facility introduces some complications to the data
maintaining process due to the lifetime. Since the data is usually distributed
among few heterogeneous end-points, we can not rely just on `rsync` utility and
call it a day.

Besides of the CASTOR complications (which will vanish soon, as it will be
immersed into CERN EOS facility), the goal of the `castlib` is to provide a
flexible interfaces to deal the data retrieval and maintenance by means of
RESTful API:

- Getting list of indexed files located on certain end-point
- Provide data transfer (and sync) between end-points
- Provide selective retrieval of certain events
- Establish a task-queuing procedure for data production

This needs leads us to some key concepts listed below.

## Filesystem Entries

`castlib4` deals with distributed data, probably located on different hosts.
Each piece of data is typically represented by one or few files physically
written on some carrier. Basically, we imply that each piece ("file") must have
a `name` that is unique in some "folder".

Besides of a mandatory `name` attribute we imply that there may be a `size`,
`modification date` and so on.

## Backend

All the attributes are reachable via corresponding `Backend` interface instance
which defines how `castlib4` has to interact with the node of certain type.

Backend instance must define features supported by the storaging service
(hashsum check, copying, moving, renaming, timestamp modification, etc) how to
handle given URI and:

- List entries names in folder (directory or catalogue)
- Move, remove, rename file or folder
- If possible, request: size, access/modification date, hashsum, access
permissions, etc.
- If possible, set access/modification date, access permissions, etc.
- Create/delete folder

Transfer between nodes is defined by dedicated classes.

## Node

Node is merely a "root" location for some filesystem tree. It is identified by
unique name, listed in config file. E.g., for the local directory the node will
be identified by a path (literally, e.g., `/some/where`).

In more complicated cases (remote location, reachable by some protocol like
SSH or CASTOR), node credentials may be more complicated.

# RESTful API drafts

* The `/data/events` provides experiment-specific customized interface for
per-event retrieval.
* The `/data/files` provides direct, filesystem-like listings representing data
arranged "invariant path":

    GET => http://somehost.cern.ch/data/na64
    <= {
        "cdr11011-1988.dat" : {
            ...
        },
        ...
    }

    GET => http://somehost.cern.ch/data/na64/cdr11011-1988.dat
    <= ... the cdr11011-1988.dat file as is ...

* The `/api4/tasks` represents an interface to task queue, with
list/create/deny/status lifecycle:
    1. `GET => /api4/task` returns list of queued tasks
    2. `GET => /api4/task/<taskID>` returns the status of task identified
by `taskID`
    3. `POST => /api4/task` tries to push new task to queue returning its ID
    4. `DELETE => /api4/task/<taskID>` tries to remove the task from the queue
or abort currently running.

This tasks are actually a Celery tasks.


.. _faq:

FAQ
==========

How do I cite Cosmos?
    Cosmos was officially published as a
    `manuscript <http://bioinformatics.oxfordjournals.org/content/early/2014/06/29/bioinformatics.btu385>`_,
    but has evolved a lot since it's original inception.  If you use Cosmos
    for research, please cite it's `manuscript <http://bioinformatics.oxfordjournals.org/content/early/2014/06/29/bioinformatics.btu385>`_.  This means a lot to the author.


Is there an easy way to get the command that was executed to run a pipeline?
    Yes, check the primary log output, it will be the first thing that Cosmos writes to it.

How can I compose Workflows together?
    We do this by making "Recipes", which is not a Cosmos primitive, but rather simply a function that takes a Workflow and modifies it's DAG.  Recipes
    can easily call other Recipes since they are just functions.  Because Recipes can often require complex input datastructures, we like to create Recipe Input Schemas using
    `Voluptuous <https://github.com/alecthomas/voluptuous>`_

How can I be more efficient with I/O?  Writing all the files back and forth to our shared filesystem has become a bottle neck.
    This is the most common failure point for large production workflows on a traditional cluster (Hadoop and Spark get around this by using HDFS, but then
    you are limited to the map reduce framework).  To reduce the amount of shared filesystem I/O of your pipeline, you can make sub-pipelines that are themselves jobs, that run using drm='local' on
    a single node, utilize (fast) disk local on the node for scratch space, and only push final results back to the shared file system.

    For example, a child Workflow using drm='local'
    might contain 10 Tasks.  The first Task would pull a large file to local disk that Tasks 2-8 process, reducing the number of times the large file has to be read
    *from the shared filesystem* to once since Tasks 2-8 are reading off the local disc.  The last Task will push the final output back to the shared filesystem, and likely delete or copy intermediate/temporary files.  To be clear,
    this involves a Cosmos pipeline submitting another Cosmos pipeline as a job.  This is what people do in production, and has other advantages such as modularizing
    different aspects of a pipeline.  It had the added benefit of greatly increasing raw I/O for the jobs that are reading from local disc, rather than the shared file system.

How can I modify the DAG based on the output of a Task?
    Run Workflow.run() after adding the Task that outputs the information you need to construct the rest of your DAG.  Then modify the DAG as normal using
    Workflow.add_task(), and once finished call Workflow.run() for a second time to run the rest of the Tasks.
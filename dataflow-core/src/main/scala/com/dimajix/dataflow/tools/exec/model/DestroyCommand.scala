package com.dimajix.dataflow.tools.exec.model

import com.dimajix.dataflow.execution.Executor
import com.dimajix.dataflow.spec.Project
import com.dimajix.dataflow.tools.exec.ActionCommand


class DestroyCommand extends ActionCommand {
    override def executeInternal(executor:Executor, project: Project) : Boolean = {
        false
    }
}

package com.dimajix.flowman.hub.model

import com.dimajix.flowman.hub.service


object Converter {
    def of(kernel:service.KernelService) : Kernel = {
        Kernel(kernel.id, kernel.url.map(_.toString), kernel.state.toString)
    }

    def of(launcher: service.Launcher) : Launcher = {
        Launcher(launcher.name, launcher.description)
    }
}

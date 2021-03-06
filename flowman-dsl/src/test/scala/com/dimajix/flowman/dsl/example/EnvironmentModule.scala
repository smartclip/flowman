package com.dimajix.flowman.dsl.example

import com.dimajix.flowman.dsl.Module


object EnvironmentModule extends Module {
    environment += (
        "app_name" -> "${project.name}",
        "app_version" -> "${project.version}",
        "processing_date" -> "2019-03-01",
        "test_basedir" -> "/tmp/flowman-test",
        "hdfs_landing_dir" -> "${hdfs_basedir}/landing",
        "hdfs_structured_dir" -> "${hdfs_basedir}/structured"
    )

    config += (
        "spark.sql.session.timeZone" -> "UTC",
        "spark.app.name" -> "${project.name} (${project.version})"
    )
}

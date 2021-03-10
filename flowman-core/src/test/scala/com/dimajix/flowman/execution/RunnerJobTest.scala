/*
 * Copyright 2018 Kaya Kupferschmidt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dimajix.flowman.execution

import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.dimajix.common.Yes
import com.dimajix.flowman.execution.RunnerJobTest.NullTarget
import com.dimajix.flowman.model.BaseTarget
import com.dimajix.flowman.model.Hook
import com.dimajix.flowman.model.Job
import com.dimajix.flowman.model.JobInstance
import com.dimajix.flowman.model.JobWrapper
import com.dimajix.flowman.model.Metadata
import com.dimajix.flowman.model.Namespace
import com.dimajix.flowman.model.NamespaceWrapper
import com.dimajix.flowman.model.Project
import com.dimajix.flowman.model.ProjectWrapper
import com.dimajix.flowman.model.Target
import com.dimajix.flowman.model.TargetIdentifier
import com.dimajix.flowman.model.TargetInstance
import com.dimajix.flowman.model.Template
import com.dimajix.flowman.model.Test
import com.dimajix.flowman.types.StringType


object RunnerJobTest {
    object NullTarget {
        def apply(name:String, partition: Map[String,String] = Map()) : Context => NullTarget = {
            ctx:Context => NullTarget(Target.Properties(ctx, name), ctx.evaluate(partition))
        }
    }
    case class NullTarget(
        instanceProperties: Target.Properties,
        partition: Map[String,String]
    ) extends BaseTarget {
        override def instance: TargetInstance = {
            TargetInstance(
                namespace.map(_.name).getOrElse(""),
                project.map(_.name).getOrElse(""),
                name,
                partition
            )
        }
    }
}


class RunnerJobTest extends AnyFlatSpec with MockFactory with Matchers {
    "The Runner for Jobs" should "correctly handle environments and arguments" in {
        val project = Project(
            name = "default",
            environment = Map(
                "project_env" -> "project",
                "project_env_to_overwrite" -> "project"
            )
        )
        val session = Session.builder()
            .withEnvironment("global_env", "global")
            .withEnvironment("global_env_to_overwrite", "global")
            .build()
        val context = session.getContext(project)
        val job = Job.builder(context)
            .setName("my_job")
            .addParameter("param", StringType)
            .addEnvironment("global_env_to_overwrite", "job")
            .addEnvironment("project_env_to_overwrite", "job")
            .addEnvironment("job_env", "job")
            .build()

        val args = Map(
            "param" -> "lala"
        )

        val runner = session.runner
        runner.withJobContext(job, args, force=false, dryRun=false) { (context,args) =>
            args should be (Map("param" -> "lala"))
            context.environment.toMap should be(Map(
                "param" -> "lala",
                "project_env" -> "project",
                "project_env_to_overwrite" -> "job",
                "global_env" -> "global",
                "global_env_to_overwrite" -> "global",
                "job_env" -> "job",
                "job" -> JobWrapper(job),
                "force" -> false,
                "dryRun" -> false,
                "project" -> ProjectWrapper(project),
                "namespace" -> NamespaceWrapper(None)
            ))
        }
        runner.withEnvironment(job, Phase.BUILD, args, force=false, dryRun=false) { environment =>
            environment.toMap should be(Map(
                "param" -> "lala",
                "project_env" -> "project",
                "project_env_to_overwrite" -> "job",
                "global_env" -> "global",
                "global_env_to_overwrite" -> "global",
                "job_env" -> "job",
                "job" -> JobWrapper(job),
                "force" -> false,
                "dryRun" -> false,
                "phase" -> "build",
                "project" -> ProjectWrapper(project),
                "namespace" -> NamespaceWrapper(None)
            ))
        }
    }

    it should "work" in {
        val session = Session.builder()
            .build()
        val job = Job.builder(session.context)
            .setName("batch")
            .build()

        val runner = session.runner
        runner.executeJob(job, Seq(Phase.BUILD)) should be (Status.SUCCESS)
        runner.executeJob(job, Seq(Phase.BUILD)) should be (Status.SUCCESS)
    }

    it should "throw exceptions on missing parameters" in {
        val session = Session.builder()
            .build()
        val job = Job.builder(session.context)
            .setName("batch")
            .addParameter("p1", StringType)
            .build()

        val runner = session.runner
        an[IllegalArgumentException] shouldBe thrownBy(runner.executeJob(job, Seq(Phase.BUILD)))
    }

    it should "catch exceptions" in {
        val session = Session.builder()
            .build()
        val job = Job.builder(session.context)
            .setName("batch")
            .addTarget(TargetIdentifier("some_target"))
            .build()

        val runner = session.runner
        runner.executeJob(job, Seq(Phase.BUILD)) should be (Status.FAILED)
    }

    it should "only execute specified targets" in {
        def genTarget(name:String, toBeExecuted:Boolean) : Context => Target = (ctx:Context) => {
            val instance = TargetInstance("default", "default", name)
            val target = mock[Target]
            (target.name _).expects().anyNumberOfTimes().returns(name)
            (target.before _).expects().anyNumberOfTimes().returns(Seq())
            (target.after _).expects().anyNumberOfTimes().returns(Seq())
            (target.phases _).expects().anyNumberOfTimes().returns(Lifecycle.ALL.toSet)
            (target.metadata _).expects().anyNumberOfTimes().returns(Metadata(name=name, kind="target", category="target"))
            (target.requires _).expects(*).anyNumberOfTimes().returns(Set())
            (target.provides _).expects(*).anyNumberOfTimes().returns(Set())
            (target.identifier _).expects().anyNumberOfTimes().returns(TargetIdentifier(name))
            (target.instance _).expects().anyNumberOfTimes().returns(instance)
            (target.dirty _).expects(*, Phase.CREATE).anyNumberOfTimes().returns(Yes)
            if (toBeExecuted)
                (target.execute _).expects(*, Phase.CREATE).returning(Unit)
            else
                (target.execute _).expects(*, Phase.CREATE).never().returning(Unit)

            target
        }
        def genJob(session:Session, project:Project) : Job = {
            Job.builder(session.getContext(session.project.get))
                .setTargets(project.targets.map(t => TargetIdentifier(t._1)).toSeq)
                .build()
        }
        def genProject(targets:Map[String, Boolean]) : Project = {
            Project(
                name = "default",
                targets = targets.map { case(name,toBeExecuted) => name -> Template.of(genTarget(name, toBeExecuted)) }
            )
        }

        {
            val project = genProject(Map("a" -> true, "ax" -> true, "b" -> false))
            val session = Session.builder()
                .withProject(project)
                .build()
            val job = genJob(session, project)
            val runner = session.runner
            runner.executeJob(job, Seq(Phase.CREATE), targets=Seq("a.*".r)) should be(Status.SUCCESS)
        }
    }

    it should "not execute targets in dryMode" in {
        def genTarget(name:String) : Context => Target = (ctx:Context) => {
            val instance = TargetInstance("default", "default", name)
            val target = mock[Target]
            (target.name _).expects().anyNumberOfTimes().returns(name)
            (target.before _).expects().anyNumberOfTimes().returns(Seq())
            (target.after _).expects().anyNumberOfTimes().returns(Seq())
            (target.phases _).expects().anyNumberOfTimes().returns(Lifecycle.ALL.toSet)
            (target.metadata _).expects().anyNumberOfTimes().returns(Metadata(name=name, kind="target", category="target"))
            (target.requires _).expects(*).anyNumberOfTimes().returns(Set())
            (target.provides _).expects(*).anyNumberOfTimes().returns(Set())
            (target.identifier _).expects().anyNumberOfTimes().returns(TargetIdentifier(name))
            (target.instance _).expects().anyNumberOfTimes().returns(instance)
            (target.dirty _).expects(*, Phase.CREATE).anyNumberOfTimes().returns(Yes)
            (target.execute _).expects(*, Phase.CREATE).never().returning(Unit)

            target
        }

        val project = Project(
            name = "default",
            targets = Map("a" -> Template.of(genTarget("a")))
        )

        val session = Session.builder()
            .withProject(project)
            .build()
        val job = Job.builder(session.getContext(session.project.get))
            .setTargets(project.targets.map(t => TargetIdentifier(t._1)).toSeq)
            .build()

        val runner = session.runner
        runner.executeJob(job, Seq(Phase.CREATE), dryRun = true) should be(Status.SUCCESS)
    }

    it should "invoke all hooks (in jobs and namespaces)" in {
        val jobHook = mock[Hook]
        val jobJobToken = new JobToken {}
        val jobTargetToken = new TargetToken {}
        (jobHook.startJob _).expects( where( (_:JobInstance, phase:Phase) => phase == Phase.BUILD) ).returning(jobJobToken)
        (jobHook.finishJob _).expects(jobJobToken, Status.SUCCESS)
        (jobHook.startTarget _).expects( where( (_:TargetInstance, phase:Phase, token:Option[JobToken]) => phase == Phase.BUILD && token == Some(jobJobToken))).returning(jobTargetToken)
        (jobHook.finishTarget _).expects(jobTargetToken, Status.SUCCESS)
        val namespaceHook = mock[Hook]
        val namespaceJobToken = new JobToken {}
        val namespaceTargetToken = new TargetToken {}
        (namespaceHook.startJob _).expects( where( (_:JobInstance, phase:Phase) => phase == Phase.BUILD) ).returning(namespaceJobToken)
        (namespaceHook.finishJob _).expects(namespaceJobToken, Status.SUCCESS)
        (namespaceHook.startTarget _).expects( where( (_:TargetInstance, phase:Phase, token:Option[JobToken]) => phase == Phase.BUILD && token == Some(namespaceJobToken))).returning(namespaceTargetToken)
        (namespaceHook.finishTarget _).expects(namespaceTargetToken, Status.SUCCESS)

        val ns = Namespace(
            name = "default",
            hooks = Seq(new Template[Hook] {
                override def instantiate(context: Context): Hook = namespaceHook
            })
        )
        val project = Project(
            name = "default",
            targets = Map("t0" -> NullTarget("t0", Map("p1" -> "$p1")))
        )
        val session = Session.builder()
            .withNamespace(ns)
            .withProject(project)
            .build()
        val job = Job.builder(session.getContext(project))
            .setName("job")
            .addHook(jobHook)
            .addParameter("p1", StringType)
            .addTarget(TargetIdentifier("t0"))
            .build()

        val runner = session.runner
        runner.executeJob(job, Seq(Phase.BUILD), Map("p1" -> "v1")) should be (Status.SUCCESS)
    }
}
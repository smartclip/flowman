# Flowman Schema Generator
The Flowman schema generator is a small utility application which generates a YAML schema file from the current
configuration including all plugins. This YAML schema file then can be used to in appropriate code editors which
support schema validation of YAML files. This will support developers in creating Flowman projects by enabling better
syntax highlighting and code auto-completion.

```shell
flowman-schema -o my-schema-directory
```

This command will create multiple different YAML schema files:
* `module.json` - This is the YAML schema for all modules, i.e. defining relations, mapping, etc.
* `project.json` - This YAML schema file contains all entities of the [`project.yml`](../spec/project.md) file.
* `namespace.sjon` - This YAML schema file contains all entities of [namespace definitions](../spec/namespace.md).
* `documentation.sjon` - This YAML schema file contains all entities of [`documentation.yml`](../documenting/config.md).


## Provided Schemas

Currently, there is no readily provided schema for Flowman available. This means that you really have to run the
command above to generate a YAML schema file for your environment. The reasoning behind not providing schema files is
simple: Since the available entities and YAML tags depend on the presence of plugins, the best solution is provide
some means to generate a YAML schema file precisely for your configuration. 


## Supported Editors

JSON schema definition files are supported by many code editors. The following editors are known to work well with
validation and auto-completion with the Flowman YAML schema files:

* JetBrains IntelliJ (and probably other IDEs from the JetBrains family)
* Microsoft Visual Studio Code (via the [YAML plugin](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml))

Please refer to the corresponding documentation how to configure each editor to use the YAML schema definitions.


### Setup JetBrains IntelliJ

Adding the JSON schema in IntelliJ is simple to do.

1. Open the settings dialog via **File > Settings...**
2. Go to **Language & Frameworks > Schemas & DTDs > JSON Schema Mappings**
3. Add a new entry, assign it a name ("Flowman Module Schema"), and assign the `module.json` schema file (Version 4)
4. Add your Flowman project directories to the newly created JSON Schema

Repeat the steps for the `project.json` and `namespace.yml`, and assign them the individual files (`project.yml` and
possibly `default-namespace.yml`).


### Setup Visual Studio Code

In order to benefit from a really excellent autocompletion in Visual Studio Code, you need to install the
[YAML plugin](https://marketplace.visualstudio.com/items?itemName=redhat.vscode-yaml). Then you can easily add

1. Open your Visual Studio Code `settings.json` file
2. Add appropriate mappings for the Flowman YAML schema files:
```json
{
    "yaml.schemas": {
        "/home/kaya/Dimajix/flowman-schema/project.json": "flows/**/project.yml",
        "/home/kaya/Dimajix/flowman-schema/documentation.json": "flows/**/documentation.yml",
        "/home/kaya/Dimajix/flowman-schema/module.json": "flows/*/*/*.yml"
    }
}
```

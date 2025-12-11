### Building a Go program against a local DuckDB (for debugging)

To debug into DuckDB's C++ sources from a Go application that uses `duckdb-go`, you need to link your Go binary against the locally built
DuckDB debug lubrary. You also need to set the correct rpath so that the dynamic loader can find the S++ sources.

A helper script is provided:
```
scripts/go-build-duckdb-debug.sh /absolute/path/to/your/go/app [package_or_dir] [output_name]
```

Example:
```
# Build a debug build from the DuckDB repo root:
make debug

# Build the Go application, linking to this DuckDB repo's debug library (`libduckdb`)
scripts/go-build-duckdb-debug.sh ~/Development/example-duckdb-go-project ./examples/simple simple_debug
```
The script will export `CGO_CFLAGS` and `CGO_LDFLAGS` appropriately (including `-Wl,-rpath,...`) and invoke `go build -tags duckdb_use_lib -gcflags "all=-N -l"` so you can step through DuckDB code in a debugger.
If the loader cannot find `libduckdb` at runtime, set `DYLD_LIBRARY_PATH` (macOS) or `LD_LIBRARY_PATH` (Linux) to `build/debug/src` of this repository.

### Debugging in CLion
#### In Clion:
1. Import the following build configuration, you can replace `./../example-duckdb-go-project` with your own Go project path. This configuration will act as a helper for the next configuration.
```xml
<component name="ProjectRunConfigurationManager">
  <configuration default="false" name="build-go-debug" type="ShConfigurationType">
    <option name="SCRIPT_TEXT" value="" />
    <option name="INDEPENDENT_SCRIPT_PATH" value="true" />
    <option name="SCRIPT_PATH" value="$PROJECT_DIR$/scripts/go-build-duckdb-debug.sh" />
    <option name="SCRIPT_OPTIONS" value="$PROJECT_DIR$/../../example-duckdb-go-project $PROJECT_DIR$/../../example-duckdb-go-project/ go_debug" />
    <option name="INDEPENDENT_SCRIPT_WORKING_DIRECTORY" value="true" />
    <option name="SCRIPT_WORKING_DIRECTORY" value="$PROJECT_DIR$" />
    <option name="INDEPENDENT_INTERPRETER_PATH" value="true" />
    <option name="INTERPRETER_PATH" value="/bin/zsh" />
    <option name="INTERPRETER_OPTIONS" value="" />
    <option name="EXECUTE_IN_TERMINAL" value="true" />
    <option name="EXECUTE_SCRIPT_FILE" value="true" />
    <envs />
    <method v="2" />
  </configuration>
</component>
```
2. Replace the script path of `go-build-duckdb-debug.sh` with the path to your local copy of the script.
3. Import the `go-debug` configuration, and again, replace `./../example-duckdb-go-project` with your own Go project path.
```xml
<component name="ProjectRunConfigurationManager">
    <configuration default="false" name="go-debug" type="CLionExternalRunConfiguration" factoryName="Application" REDIRECT_INPUT="false" ELEVATE="false" USE_EXTERNAL_CONSOLE="false" EMULATE_TERMINAL="false" PASS_PARENT_ENVS_2="true" PROJECT_NAME="DuckDB" TARGET_NAME="duckdb" CONFIG_NAME="duckdb" RUN_PATH="$PROJECT_DIR$/../../example-duckdb-go-project/go_debug">
        <method v="2">
            <option name="CLION.EXTERNAL.BUILD" enabled="true" />
            <option name="RunConfigurationTask" enabled="true" run_configuration_name="build-go-debug" run_configuration_type="ShConfigurationType" />
        </method>
    </configuration>
</component>
```
4. Run the `go-debug` configuration in debug mode. If everything is set up correctly, you should be able to step into DuckDB C++ code from your Go application.
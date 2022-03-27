[![NuGet](https://img.shields.io/nuget/v/NLog.Targets.ClickHouse.svg)](https://www.nuget.org/packages/NLog.Targets.ClickHouse)
[![NuGet downloads](https://img.shields.io/nuget/dt/NLog.Targets.ClickHouse.svg)](https://www.nuget.org/packages/NLog.Targets.ClickHouse)

# NLog.Targets.ClickHouse
Implementation of ClickHouse target for Nlog.

## Installation

```shell
Install-Package NLog.Targets.ClickHouse -Version 1.1.0
```

## Example configuration:
The ClickHouse target works best together with the [BufferingWrapper](https://github.com/NLog/NLog/wiki/BufferingWrapper-target) target applied.

```xml
<?xml version='1.0' encoding='utf-8' ?>
<nlog xmlns='http://www.nlog-project.org/schemas/NLog.xsd'
      xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'
      internalLogToConsole='true'
      autoReload='true'>
    <targets>
        <target
                name='clickhouse_queue'
                xsi:type='BufferingWrapper'
                bufferSize='10000'
                flushTimeout='5000'
                overflowAction='Flush'>
            <target name='clickhouse'
                    xsi:type='ClickHouse'
                    connectionString='Host=localhost;Port=9000;Database=logs;User=sa;Password=P@ssw0rd;'
                    tableName='test'
                    batchSize='5000'>
                <install-command text="
                    CREATE TABLE IF NOT EXISTS test
                    (
                        logged DateTime64,
                        application String,
                        level LowCardinality(String),
                        message String,
                        logger String,
                        callSite String,
                        exception Nullable(String)
                    )
                    ENGINE Log" />
                <install-command text="TRUNCATE TABLE test" />
                <uninstall-command text="DROP TABLE IF EXISTS test" />
                <parameter name='application' dbType='String' layout='test_app'/>
                <parameter name='level' dbType='String' layout='${level}' />
                <parameter name='message' dbType='String' layout='${message}' />
                <parameter name='logger' dbType='String' layout='${logger}' />
                <parameter name='callSite' dbType='String' allowDbNull='true' layout='${callsite:filename=true}' />
                <parameter name='exception' dbType='String' allowDbNull='true' layout='${exception:tostring}' />
                <parameter name='logged' dbType='DateTime64' layout='${date}' />
            </target>
        </target>
    </targets>
    <rules>
        <logger name='*' minlevel='Trace' writeTo='clickhouse_queue' />
    </rules>
</nlog>
```

## Development plans:
- [x] add internal queue in target to use instead of BufferedWrapper
- [ ] improvements of default behavior;
- [ ] initialization query feature to create or clean table and auto-create table support;
- [ ] improvements and expansion of the config parameters list;
- [ ] improved testing and benchmarking;

## Special thanks
Many thanks to the authors of the implementations ClickHouse .NET client [killwort/ClickHouse-Net](https://github.com/killwort/ClickHouse-Net) and [DarkWanderer/ClickHouse.Client](https://github.com/DarkWanderer/ClickHouse.Client).
It was a challenge to choose which implementation to use.

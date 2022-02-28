# NLog.Targets.ClickHouse
Implimentation of ClickHouse target for Nlog.

## Development plans:
- improvements of default behavior;
- initialization query feature to create table and auto-create table support;
- improvements and expansion of the config parameters list;
- improved testing and benchmarking;

## Example configuration:
The ClickHouse target works best with the [BufferingWrapper](https://github.com/NLog/NLog/wiki/BufferingWrapper-target) target applied.

```xml
<nlog xmlns='http://www.nlog-project.org/schemas/NLog.xsd'
      xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'
      internalLogToConsole='true'>
    <targets>
        <extensions>
            <add assembly="NLog.Targets.ClickHouse"/>
        </extensions>
        <target
                name='clickhouse_queue'
                xsi:type='BufferingWrapper'
                bufferSize='10000'
                flushTimeout='5000'
                overflowAction='Flush'>
            <target name='clickhouse'
                    xsi:type='ClickHouse'
                    connectionString='Host=localhost;Port=8123;Username=sa;Password=P@ssw0rd;Database=logs'
                    tableName='logs.test'
                    batchSize='5000'>
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

## Special thanks
Many thanks to the authors of the implementations ClickHouse .NET client [DarkWanderer/ClickHouse.Client](https://github.com/DarkWanderer/ClickHouse.Client) and [killwort/ClickHouse-Net](https://github.com/killwort/ClickHouse-Net).
It was a challenge to choose which implementation to use. Maybe will implement both via interface and config options.

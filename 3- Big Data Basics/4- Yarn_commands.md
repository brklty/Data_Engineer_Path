# Yarn Commands

spark initiation by sourcing from yarn:

```
pyspark --master yarn
```

Demonstrate YARN applications

```
yarn application -list
```

Demonstrate knowledge of YARN applications

```
yarn application status <Application ID>
```

Stopping YARN applications

```
yarn application -kill <Application ID>
```

Retrieving log files of YARN applications

```
yarn logs -applicationId <ApplicationID> > application.txt
```
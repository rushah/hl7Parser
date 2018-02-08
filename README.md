# hl7Parser
A simple hl7Parser processor that parses an hl7 message into an SDC Record.

##To get started

1. Download StreamSets Data Collector (SDC) or use an existing instance (SDC version 3.x and above)
```
git clone https://github.com/rushah/hl7Parser.git
```
2. Change directory into the hl7Parser

3. Execute a maven clean install
```mvn clean install
```

A build success message should appear
```
...
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 9.874 s
[INFO] Finished at: 2018-02-08T12:07:24-08:00
[INFO] Final Memory: 31M/418M
[INFO] ------------------------------------------------------------------------
```

4. Navigate to the $SDC_DIST/user_libs directory and extract the hl7Parser binaries into user_libs
```
tar xf ~/hl7Parser/target/hl7Parser-1.0-SNAPSHOT.tar.gz
```

5. Navigate to $SDC_CONF folder to update the sdc-security.policy file and provide permissions to user_libs. Add the following lines to the sdc-security.policy file
```
grant codebase "file://${sdc.dist.dir}/user-libs/-" {
  permission java.security.AllPermission;
};
```

6. Save the file and restart StreamSets Data Collector

Caution: This is a beta version, but please feel free to provide any feedback.

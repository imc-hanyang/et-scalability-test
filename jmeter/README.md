```bash
cd results/_MB/__
```

```bash
jmeter -n -t ../../et-grpc-login-test.jmx -l res.csv
```

```bash
jmeter -g res.csv -o report
```
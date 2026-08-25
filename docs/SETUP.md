# Environment Setup

Everyone on the team needs Java, Hadoop, and Spark installed and running before touching the pipeline code. Pick your OS below. Windows users: use WSL (Windows Subsystem for Linux) — Hadoop does not run well natively on Windows, so we treat WSL as "Linux" for this setup.

**Versions used here:** Hadoop 3.5.0, Spark 3.5.9. Spark also has a newer 4.x line, but we stay on 3.5.x — it's the long-term-support branch (patched through late 2027) and has far more tutorials/Stack Overflow coverage than 4.x, which matters more for a course project than being on the newest release.

---

## Linux

### 1. Install Java
```bash
sudo apt update && sudo apt install openjdk-11-jdk -y
java -version
```
Find your Java install path (you'll need it below):
```bash
sudo update-alternatives --config java
```
Copy the path shown, minus the trailing `/bin/java`. Example: `/usr/lib/jvm/java-11-openjdk-amd64`.

### 2. Download and configure Hadoop
```bash
wget https://downloads.apache.org/hadoop/common/hadoop-3.5.0/hadoop-3.5.0.tar.gz
tar -xzf hadoop-3.5.0.tar.gz -C ~/
mv ~/hadoop-3.5.0 ~/hadoop
```
Add to `~/.bashrc`:
```bash
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=~/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```
```bash
source ~/.bashrc
```
Edit `$HADOOP_HOME/etc/hadoop/core-site.xml` — set `fs.defaultFS` to `hdfs://localhost:9000`.
Edit `$HADOOP_HOME/etc/hadoop/hdfs-site.xml` — set `dfs.replication` to `1`.

### 3. Set up passwordless SSH to localhost
Hadoop's start scripts need this even on a single machine.
```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ssh localhost   # should log in with no password prompt
```

### 4. Format and start HDFS
```bash
hdfs namenode -format   # only run this once, ever
start-dfs.sh
hdfs dfs -ls /          # confirm it's alive
hdfs dfs -mkdir -p /raw /curated /marts
```
You can also check `http://localhost:9870` in a browser.

### 5. Download and configure Spark
```bash
wget https://downloads.apache.org/spark/spark-3.5.9/spark-3.5.9-bin-hadoop3.tgz
tar -xzf spark-3.5.9-bin-hadoop3.tgz -C ~/
mv ~/spark-3.5.9-bin-hadoop3 ~/spark
```
Add to `~/.bashrc`:
```bash
export SPARK_HOME=~/spark
export PATH=$PATH:$SPARK_HOME/bin
```
```bash
source ~/.bashrc
```

### 6. Test the whole chain
```bash
spark-shell
```
Inside the Scala prompt:
```scala
spark.range(5).write.csv("hdfs://localhost:9000/raw/test")
```
Then confirm:
```bash
hdfs dfs -ls /raw/test
```

---

## macOS

### 1. Install Java
```bash
brew install openjdk@11
```
Add to `~/.zshrc`:
```bash
export JAVA_HOME=$(/usr/libexec/java_home -v11)
```
```bash
source ~/.zshrc
java -version
```

### 2. Install Hadoop and Spark via Homebrew
```bash
brew install hadoop spark
```
Homebrew installs these as ready-to-run binaries — no manual download/unzip needed like on Linux. Find where Homebrew put them:
```bash
brew --prefix hadoop
brew --prefix spark
```
Add to `~/.zshrc`:
```bash
export HADOOP_HOME=$(brew --prefix hadoop)/libexec
export SPARK_HOME=$(brew --prefix spark)/libexec
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin
```
```bash
source ~/.zshrc
```

### 3. Configure Hadoop
Edit `$HADOOP_HOME/etc/hadoop/core-site.xml` — set `fs.defaultFS` to `hdfs://localhost:9000`.
Edit `$HADOOP_HOME/etc/hadoop/hdfs-site.xml` — set `dfs.replication` to `1`.

### 4. Enable Remote Login (Hadoop needs local SSH)
System Settings → General → Sharing → turn on **Remote Login**.
```bash
ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
ssh localhost
```

### 5. Format and start HDFS
```bash
hdfs namenode -format   # only once, ever
start-dfs.sh
hdfs dfs -ls /
hdfs dfs -mkdir -p /raw /curated /marts
```

### 6. Test the whole chain
```bash
spark-shell
```
```scala
spark.range(5).write.csv("hdfs://localhost:9000/raw/test")
```
```bash
hdfs dfs -ls /raw/test
```

---

## Windows (via WSL)

Hadoop doesn't run reliably on native Windows, so install WSL first, then follow the **Linux steps above inside it.**

### 1. Install WSL
In PowerShell (as Administrator):
```powershell
wsl --install
```
Restart when prompted. This installs Ubuntu by default. Open the "Ubuntu" app from the Start menu — you're now in a Linux terminal.

### 2. Follow the Linux instructions above
From here, every step (Java, Hadoop, SSH, HDFS, Spark) is identical to the Linux section — run it all inside the WSL/Ubuntu terminal, not PowerShell or Command Prompt.

### A note on files
Keep the repo and all Hadoop/Spark files inside the WSL file system (e.g. `~/projects/...`), not on the Windows `C:\` drive (`/mnt/c/...`). Files on the Windows drive are much slower to access from WSL and can cause permission issues with Hadoop.

---

## Confirming you're ready

Everyone should be able to run this and see it succeed before writing any pipeline code:
```bash
hdfs dfs -ls /raw /curated /marts
spark-shell --version
```

---

# Testing

We don't test against a real multi-node cluster — everything is tested against **local Spark and local HDFS**, the same setup from above.

### What gets tested
- **Encryption (TASK-3):** a plain `pytest` suite. Encrypt a value, decrypt it, check it matches the original. Check the encrypted value looks nothing like the original.
- **Access control (TASK-4):** `pytest` again. Call `get_view()` with each role, check the right columns come back decrypted or hidden. Check a denied request is actually denied and logged.
- **ETL (TASK-2):** Spark's own local mode (`local[*]`) makes this testable without a real cluster. Feed a small fake DataFrame with a few good rows and a few deliberately broken ones, run the cleaning function on it, and check the good rows end up in the output and the bad rows end up in the rejects with a reason.
- **Aggregates (TASK-5):** same idea — feed a small known DataFrame in, check the summary numbers coming out are what you'd expect by hand.
- **Ingestion (TASK-1):** test against a small folder of fake CSVs, not real Synthea output — check the manifest has the right row counts and a broken file gets skipped, not loaded.

### Where tests live
```
security/test_encryption.py
security/test_access_control.py
etl/test_validate_and_clean.py
analytics/test_aggregates.py
ingestion/test_load_to_hdfs.py
```
Run them all with:
```bash
pytest
```


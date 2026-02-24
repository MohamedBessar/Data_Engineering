# 🐧 Linux Commands Reference for Data Engineers

This document contains all Linux commands used during the session.
Each command includes a short description and example usage.

---

# 1️⃣ Navigation Commands

## pwd
Print current working directory.

```bash
pwd
```

## ls
List directory contents.

```bash
ls
```

Common options:

```bash
ls -l      # detailed view
ls -a      # include hidden files
ls -lh     # human-readable sizes
```

## cd
Change directory.

```bash
cd folder_name
cd ..
cd ~
```

---

# 2️⃣ File & Directory Management

## mkdir
Create directory.

```bash
mkdir data
mkdir -p project/raw
```

## touch
Create empty file.

```bash
touch file.txt
```

## cp
Copy files or directories.

```bash
cp file.txt backup.txt
cp -r folder1 folder2
```

## mv
Move or rename files.

```bash
mv file.txt newname.txt
mv file.txt /path/to/destination/
```

## rm
Remove files or directories.

```bash
rm file.txt
rm -r folder/
rm -rf folder/
```

---

# 3️⃣ Viewing Files

## cat
Display entire file content.

```bash
cat data.csv
```

## less
View large file safely.

```bash
less bigfile.log
```

Controls:
- q → quit
- /text → search

## head
Show first lines.

```bash
head data.csv
head -n 20 data.csv
```

## tail
Show last lines.

```bash
tail data.csv
tail -f app.log
```

---

# 4️⃣ Searching & Text Processing

## grep
Search text patterns.

```bash
grep ERROR app.log
grep -i error app.log
grep -r "SELECT" .
```

## wc
Count lines, words, characters.

```bash
wc file.txt
wc -l file.txt
```

## sort
Sort lines.

```bash
sort file.txt
sort -n numbers.txt
```

## uniq
Remove duplicate lines.

```bash
sort file.txt | uniq
```

## cut
Extract columns from delimited file.

```bash
cut -d, -f2 data.csv
```

## awk
Column processing tool.

```bash
awk '{print $1}' file.txt
awk -F, '{print $2}' data.csv
```

---

# 5️⃣ Pipes & Redirection

## Pipe |
Send output of one command to another.

```bash
cat data.csv | grep 2026
```

## Redirect Output >
Overwrite file with command output.

```bash
ls > files.txt
```

## Append Output >>
Append command output to file.

```bash
echo "new row" >> data.txt
```

## Redirect Errors 2>

```bash
python script.py 2> error.log
```

## Redirect Output & Errors Together

```bash
python script.py > output.log 2>&1
```

---

# 6️⃣ Process Management

## ps
Show running processes.

```bash
ps aux
```

## top
Real-time system monitoring.

```bash
top
```

## kill
Terminate process.

```bash
kill PID
kill -9 PID
```

---

# 7️⃣ Disk & System Monitoring

## df
Display disk usage.

```bash
df -h
```

## du
Show directory size.

```bash
du -sh folder/
```

## /proc (Virtual System Files)

```bash
cat /proc/cpuinfo
cat /proc/meminfo
```

---

# 8️⃣ Permissions

## chmod
Change file permissions.

```bash
chmod 755 script.sh
chmod +x script.sh
```

## chown
Change file ownership.

```bash
sudo chown user:user file.txt
```

---

# Example: Simple Log Analysis

```bash
echo "ERROR connection failed" > app.log
echo "INFO job started" >> app.log
echo "ERROR timeout" >> app.log

grep ERROR app.log | wc -l
```

---

End of Linux Commands Reference.

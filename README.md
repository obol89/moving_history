# moving_history

Script moves 1000 records with every run. It takes records oldest than 365 days.

The script takes the following parameters:

- --db1
- --user_db1
- --host_db1
- --pass_db1
- --db2
- --user_db2
- --host_db2
- --pass_db2

If the user doesn't need password we have to proide an empty password that looks like that:
--pass_db1=""

The best option to start script is to run it through crontab.

```bash
crontab -e
```

```bash
*/1 * * * * /usr/bin/python3 /root/moving_history/moving_history.py --db1="db1" --user_db1="user_1" --host_db1="192.168.0.1" --pass_db1="" --db2="db2" --user_db2="user2" --host_db2="192.168.0.2" --pass_db2=""
```

The best approach is to set that on source server.

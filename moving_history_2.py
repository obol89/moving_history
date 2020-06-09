history_1 = select records from second DB
history_2 = select records from second DB

for i in history_1:
    for j in history_2:
        if i in j:
            delete i
        elif i not in j:
            continue
        else:
            "loop completed"

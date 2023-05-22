# simdondar_e601_tcp

program untuk koneksi antara cobas 6000 dengan sistem simdondar

untuk penggunaan download file rilis

compile dari source code (windows)

1. install Python 3.6 versi 64 bit download dari: `https://www.python.org/ftp/python/3.6.8/python-3.6.8-amd64.exe` install and add PATH di envirotment variable dan for all user check box checked.
2. clone repository, install git dulu dari: `https://git-scm.com/downloads` kemudian ketik: `git clone https://github.com/yoser174/simdondar_e601_tcp.git` pada cmd (command)
3. install requirement.txt dengan `pip install -r ./requirements.txt`
4. install mysql client library: `pip install .\package\mysqlclient-1.4.6-cp36-cp36m-win_amd64.whl`
5. run driver: `python run_driver.py`

konfigurasi:
edit file run_driver.ini, sesuaikan isi nya.

```
[General]
tcp_host = 127.0.0.1    # IP moxa
tcp_port = 5000         # port moxa
server = 192.168.0.100  # IP server simondar
db_offline = False
```

untuk edit reagent / test code
edit file `cobas6k.db` dengan SQLite browser `https://sqlitebrowser.org/`
table `ALL_SET_TEST`
edit column `test_no` sesuaikan kode reagent dengan yg digunakan di alat pada menu `system / utilities / host code`

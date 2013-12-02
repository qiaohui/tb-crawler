#!/bin/sh

# from sdl-guang-script1
cd /space/script

/usr/local/bin/post.py --db guangbi --dbuser guangbi --dbpasswd guangbi --fromdb --dbhost 192.168.33.161 --nouse_paperboy --pbcate shuoshuo --pbverbose warning --loop --use_logfile --commitfail --verbose info --timer --statshost 192.168.32.157 --daemon

/usr/local/bin/qq_login_proxy.py 8025 --verbose debug --use_logfile --dbhost 192.168.33.161 --db guangbi --dbuser guangbi --dbpasswd guangbi --webdebug --qqhost test.qq.com --daemon


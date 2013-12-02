#!/bin/sh

DBHOST="192.168.32.10"
STATHOST="192.168.32.157"
ITEMID=$1

crawl_item.py --verbose info --stderr --color --itemid $ITEMID --statshost $STATHOST --dbhost $DBHOST
crawl_image.py --verbose info --stderr --color --itemid $ITEMID --removetmp --statshost $STATHOST --dbhost $DBHOST

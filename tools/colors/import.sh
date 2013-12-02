#!/bin/sh
sqoop import --connect jdbc:mysql://sdl-guang-db3:3306/guang -username guang -password guang --table item_image_digest --target-dir /user/chris/image_digest

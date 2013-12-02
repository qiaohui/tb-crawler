#!/bin/sh

SRC_PATH='/space/wwwroot/image.guang.j.cn/ROOT/images/itemimg/0'
DST_PATH='/space/wwwroot/image.guang.j.cn/ROOT/org_images/itemimg/0'
for d in `ls $SRC_PATH`; do
    echo "coping $SRC_PATH/$d -> $DST_PATH/$d"
    rsync -aplx --link-dest=$SRC_PATH/$d $SRC_PATH/$d $DST_PATH/$d
    echo "removing $SRC_PATH/$d"
    rm -rf $SRC_PATH/$d
done

SRC_PATH1='/space/wwwroot/image.guang.j.cn/ROOT/images'
DST_PATH1='/space/wwwroot/image.guang.j.cn/ROOT/org_images'
for d in `ls -d $SRC_PATH1/*/big`; do
    DST_BIGFILE=${d/images/org_images}
    DST_BIGFILE=${DST_BIGFILE/\/big/}
    echo "coping $d -> $DST_BIGFILE"
    mkdir -p $DST_BIGFILE
    rsync -aplx --link-dest=$d $d $DST_BIGFILE
    echo "removing $d"
    rm -rf $d
done


#! /usr/bin/env python
# coding: utf-8

import os
import glob
import Image
import cStringIO
from os.path import join, getsize

filelist = glob.glob("/Users/qiaohui/Downloads/test/*.jpg")

for f in filelist:
    image = Image.open(cStringIO.StringIO(open(f).read()))
    width, height = image.size
    print width, height

    if width >= height:
        print 1
        os.system("convert -resize x300 -strip -density 72x72 -gravity center -extent 300x300 %s %s" % (f, f))
    else:
        print 2
        os.system("convert -resize 300x -strip -density 72x72 -gravity North -extent 300x300 %s %s" % (f, f))

#!/bin/sh

ps ax | grep [c]rawl_image | awk '{print $1}' | xargs kill -9


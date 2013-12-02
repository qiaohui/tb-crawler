Guang log Related scripts
===

## run @ sdl-bi2


### clicklog.py
  * 输入 /space/log/filtered/click*/click-
  * 输出 从点击日志中找到guang的点击日志，根据creative_id匹配到逛的item_id，插入到 click_item_log 表，并且输出到json文件

	python clicklog.py --stderr --color --verbose debug --commit --dbhost 192.168.32.10 --start "-15 day" --end "today"

### estimate_click2pay.py
 * 输入 clicklog.py 输出的click日志json文件，每个点击从creative_id查找item_id
 * 输入 从淘宝报表下来的xls转换的csv文件
 * 分别计算click和pay里的销量在不同离散区间的分布，然后用plot画出两者的关系

### match_outercode.py

 * pay2click 从taobao_report取得用户订单，从stat找对应的conversion出点，再从出点creative_id对回出点的item_id

	python match_outercode.py --pay2click

 * click2pay 从stat数据库的conversion表找到点击，然后通过outer_code到taobao_report表查找成交，输出 price,volume,score --> 成交 (sample.txt)

	python match_outercode.py --nopay2click

### trainer.py

利用从click2pay生成的输出文件，把联系变量离散化然后逻辑回归训练

### convert_xls.py

把下载的淘宝客报表转换为csv格式，需要安装rbco.msexcel和pyExcelerator

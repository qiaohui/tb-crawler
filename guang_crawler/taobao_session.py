#coding:utf-8

from mechanize import ParseResponse, urlopen, urljoin

url = "http://container.api.taobao.com/container?appkey=12525923"

response = urlopen(url)
import pdb; pdb.set_trace()
forms = ParseResponse(response, backwards_compat=False)

form = forms[0]
#print form

form['TPL_username'] = "简单网tb"
form['TPL_password'] = '123456tb'
form['ua'] = '094oHZxdnh4fH55dXp7eX56d6I=|oHdxoGeYpqumt65ncXpzdnFnkqaoZ6Ki|oHhxoHd3cX1xdnl5dXF7fnhxdnl5dXF+dXVxdnl5dXF9fHmiog==|oH1xZ2ei|oH5xZ2ei|oHZ2caBnrbm5tbh/dHSxtKyus3O5prSnprRzqLSydLKqsqeqt3SxtKyus3OvrbmysYS3qqmut6qouZqXkYKtubm1aniGaneLaneLsr5ztLWqs3O5prSnprRzqLSyaneLprq5rWp3i6a6ua20t66/qnOtubJqeIumtbWwqr5qeIl2d3p3en53eGdxZ2eiog==|oHZ3cWextKyus2ei|oHpxoGePpJimq6qRtKyus4itqqiwZ3GgfXd8cXd6d6JxdXFnZ3F3dXV8oqI=|oHpxoGdncaB9fHVxdn15onF1cWdncXh9e3aiog==|oHpxoGdncaB2eHl2cXd6dqJxd3FnZ3F5fnt5oqI=|oHpxoGdncaB8fnZxdnt8onF1cWdncXh9dnp3oqI=|oHpxoGdncaB8fX1xdn12onF1cWdncXh9fnZ8oqI=|oHxxoGeZlZGkuriqt7OmsqqkdmdxdnF4fnV7eKKi|oHpxoGdncaB8fX1xdn12onF3cWdncXh+eHh1oqI=|oHxxoGeZlZGkuriqt7OmsqqkdmdxdXF4fnh4fqKi|oHpxoGdncaB2d3V8cXd5e6Jxd3FnZ3F2enh7dneiog=='

request2 = form.click()
response2 = urlopen(request2)
data = response2.read()
print data


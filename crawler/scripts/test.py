from datetime import datetime, timedelta, date
import time
import random
import json

if __name__ == '__main__':
    # print(random.random(1000, 10000))
    tmp = "{&quot;artist_id&quot;:null,&quot;brand&quot;:&quot;Veronica Beard&quot;,&quot;category_id&quot;:&quot;5&quot;,&quot;designer_id&quot;:&quot;1413&quot;,&quot;name&quot;:&quot;Renzo High-Rise Pants w/ Tags&quot;,&quot;original_price&quot;:12500,&quot;position&quot;:14,&quot;price&quot;:12500,&quot;product_id&quot;:&quot;9632132&quot;,&quot;product_state&quot;:&quot;A&quot;,&quot;quantity&quot;:1,&quot;sku&quot;:&quot;WV141601&quot;,&quot;source&quot;:&quot;sale_plp&quot;}"
    tmp = tmp.replace('&quot;', '"')
    print(tmp)
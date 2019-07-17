from collections import defaultdict

LiveName='直播'
def GetLiveProduct(db, pidList):
    liveDic = defaultdict(str)
    if len(pidList) == 0:
        return liveDic

    sql = "select t.product_id,t.is_show,t.serial from t_activity_product t where t.product_id in (%s) and  t.activity_id in (select id from t_activity where group_name='%s')"%(','.join(pidList),LiveName)
    results = db.getResults(sql)
    for row in results:
        pid = int(row['product_id'])
        is_live = int(row['is_show'])
        serial = int(row['serial'])
        if is_live:
            value_1 = LiveName + str(serial)
            value_2 = value_1 + "号"
            liveDic[pid] = "%s %s %s"%(LiveName, value_1, value_2)
    return liveDic


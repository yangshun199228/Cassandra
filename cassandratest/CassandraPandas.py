from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from urllib import parse
from cassandra.auth import PlainTextAuthProvider
from clickhouse_driver import Client
import pandas as pd
import datetime
import math
import logging

logging.basicConfig(  # 通过具体的参数来更改logging模块默认行为；
    level=logging.INFO,  # 设置告警级别为INFO；
    format="%(asctime)s---%(lineno)s----%(name)s: %(message)s",  # 自定义打印的格式；
    filename="logging" + datetime.datetime.strftime(datetime.date.today(), "%Y-%m-%d") + ".txt",  # 将日志输出到指定的文件中；
    filemode="a",  # 以追加的方式将日志写入文件中，w是以覆盖写的方式哟;
)


def transformVehicleNo(vehilceNo):
    # 车牌号解码
    vehicleNoInfo = parse.unquote(vehilceNo, "gbk")
    return vehicleNoInfo[2:]


def staticsSpeed(dataFrame):
    # 获取速度相关参数
    try:
        maxSpeed = 0
        minSpeed = 0
        avgSpeed = 0
        if not dataFrame.empty:
            maxSpeed = int(dataFrame.loc[:, "gps_speed"].max())
            minSpeed = int(dataFrame.loc[:, "gps_speed"].min())
            avgSpeed = int(dataFrame.loc[:, "gps_speed"].mean())
        return [maxSpeed, minSpeed, avgSpeed]
    except Exception as e:
        logging.error(e)


def staticsAlarmCode(alarmCode):
    # 获取报警相关参数
    abnormalAlarm = 0
    speedAlarm = 0
    fatigueAlarm = 0
    violationNight = 0
    yawAlarm = 0
    for oneString in alarmCode:
        # todo
        oneList = list(str(oneString))
        if '0' in oneList:
            abnormalAlarm = abnormalAlarm + 1
        elif '10' in oneList:
            fatigueAlarm = fatigueAlarm + 1
        elif '41' in oneList:
            speedAlarm = speedAlarm + 1
        elif '53' in oneList:
            violationNight = violationNight + 1
        elif '210' in oneList:
            yawAlarm = yawAlarm + 1
    return [abnormalAlarm, speedAlarm, fatigueAlarm, violationNight, yawAlarm]


def getDayResult(oneDataFrame, vehicleInfo, vehilceNo):
    # 获取整天相关参数
    try:
        if vehicleInfo.empty:
            decodevehicleNo = transformVehicleNo(vehilceNo)
            companyName = ""
            vehicleType = ""
            city = ""
            isNetwork = 0
        else:
            decodevehicleNo = vehicleInfo.iat[0, 0]
            companyName = vehicleInfo.iat[0, 2]
            vehicleType = vehicleInfo.iat[0, 3]
            city = vehicleInfo.iat[0, 1]
            print(vehicleInfo.iat[0, 5])
            if str(vehicleInfo.iat[0, 5]) == "未入网":
                isNetwork = 0
            else:
                isNetwork = 1
        startTimePositioning = oneDataFrame.loc[:, "gps_time"].min()
        startTrackInfo = oneDataFrame[oneDataFrame["gps_time"] == startTimePositioning]
        startLon = int(startTrackInfo["gps_longitude"])
        startLat = int(startTrackInfo["gps_latitude"])
        endTimePositioning = oneDataFrame.loc[:, "gps_time"].max()
        endTrackInfo = oneDataFrame[oneDataFrame["gps_time"] == endTimePositioning]
        endLon = int(endTrackInfo["gps_longitude"])
        endLat = int(endTrackInfo["gps_latitude"])
        alarms = staticsAlarmCode(list(oneDataFrame.loc[:, "alarm_code"]))
        speedStatics = staticsSpeed(oneDataFrame)
        isOnline = 1
        dayResult = [decodevehicleNo, datetime.datetime.strptime(startTimePositioning, "%Y%m%d/%H%M%S").date(),
                     companyName, vehicleType, city, isNetwork,
                     datetime.datetime.strptime(startTimePositioning, "%Y%m%d/%H%M%S"), str(startLon / 600000 - 6),
                     str(startLat / 600000 - 6),
                     datetime.datetime.strptime(endTimePositioning, "%Y%m%d/%H%M%S"), str(endLon / 600000 - 6),
                     str(endLat / 600000 - 6), isOnline]
        dayResult.extend(alarms)
        dayResult.extend(speedStatics)
        return dayResult
    except Exception as e:
        logging.error(e)


def getPeriodResult(oneDataFrame, client, day):
    # 获取整天时段参数
    try:
        periodInfos = client.execute(" select * from b_period order by ID")
        allPeriodResult = []
        for periodInfo in periodInfos:
            periodDataFrame = oneDataFrame[(oneDataFrame["gps_time"] >= day + "/" + periodInfo[2]) &
                                           (oneDataFrame["gps_time"] >= day + "/" + periodInfo[3])]
            periodName = periodInfo[1]
            periodID = periodInfo[0]
            periodAlarms = staticsAlarmCode(list(periodDataFrame.loc[:, "alarm_code"]))
            periodSpeed = staticsSpeed(periodDataFrame)
            periodResult = [periodName, periodID]
            periodResult.extend(periodAlarms)
            periodResult.extend(periodSpeed)
            allPeriodResult.extend(periodResult)
        return allPeriodResult
    except Exception as e:
        logging.error(e)


def getSingleResult(vehilceNo, oneDataFrame, client, beginDateStr, vehicleInfo):
    dayRedult = getDayResult(oneDataFrame, vehicleInfo, vehilceNo)
    periodResult = getPeriodResult(oneDataFrame, client, beginDateStr)
    dayRedult.extend(periodResult)
    return tuple(dayRedult)


def queryTrackByVehicleNo(session, allVehicleNos, client, beginDateStr, allVehicleInfo):
    try:
        resultList = []
        for i in range(math.ceil(len(allVehicleNos) / 100)):
            if i == math.ceil(len(allVehicleNos) / 100):
                vehicleNos = allVehicleNos[i * 100:len(allVehicleNos)]
            else:
                vehicleNos = allVehicleNos[i * 100:(i + 1) * 100]
            sql = "select * from track_20191001 where license_plate in ('" + "\',\'".join(vehicleNos) + "') " \
                  " and  gps_time >= '" + beginDateStr + "/000000'" \
                  " and gps_time < '" + beginDateStr + "/235959' "
            print(sql)
            trackDatas = list(session.execute(sql))
            if trackDatas:
                trackDataFrame = pd.DataFrame(trackDatas)
                for vehilceNo in vehicleNos:
                    oneDataFrame = trackDataFrame[trackDataFrame.license_plate == vehilceNo]
                    if oneDataFrame.empty:
                        continue
                    else:
                        decodeVehicleNo = transformVehicleNo(vehilceNo)
                        resultList.append(getSingleResult(vehilceNo, oneDataFrame, client, beginDateStr,
                                                          allVehicleInfo[allVehicleInfo.vechileno == decodeVehicleNo]))
        return resultList
    except Exception as e:
        logging.error(e)


def connectCassendra():
    try:
        # 连接Cassandra
        clusterList = ['39.100.144.225']
        # clusterList = ['192.168.0.220']
        userInfo = PlainTextAuthProvider(username='cassandra', password='cassandra')
        cluster = Cluster(clusterList, auth_provider=userInfo)
        return cluster
    except Exception as e:
        print(e)
        logging.error(e)


def connectClickHouse():
    try:
        # 连接clickhouse
        # client = Client(host="192.168.0.221", port="9000", user="default", password="default", database="GpsInfo")
        client = Client(host="39.100.144.225", port="21900", user="default", password="default", database="GpsInfo")
        return client
    except Exception as e:
        print(e)
        logging.error(e)


def insertToClickhouse(client, insertResult):
    try:
        client.execute(
            "insert into t_Summary(vehicleNo, Vdate,companyName, vehicleType, city, isNetwork, startTimePositioning, startLon, startLat, endTimePositioning  ,"
            "endLon, endLat, isOnline, abnormalAlarmNum, violationNightDriving,	speedAlarmNum, fatigueAlarm, yawAlarmNum, "
            "maxSpeed,	minSpeed, avgSpeed,	"
            "periodOne,	periodID, periodSpeedAlarmNum, periodAbnormalAlarmNum, periodfatigueAlarm, periodYawAlarmNum, periodIsOnline,"
            "periodMaxSpeed, periodMinSpeed,	periodAvgSpeed,"
            "periodOne2, periodID2,	periodSpeedAlarmNum2, periodAbnormalAlarmNum2, periodfatigueAlarm2,	"
            "periodYawAlarmNum2, periodIsOnline2, periodMaxSpeed2, periodMinSpeed2,	periodAvgSpeed2, "
            "periodOne3  ,	periodID3,	periodSpeedAlarmNum3,periodAbnormalAlarmNum3,	periodfatigueAlarm3,"
            "periodYawAlarmNum3, periodIsOnline3,	periodMaxSpeed3,	periodMinSpeed3  ,	periodAvgSpeed3,"
            "periodOne4, periodID4,	periodSpeedAlarmNum4, periodAbnormalAlarmNum4, periodfatigueAlarm4,	"
            "periodYawAlarmNum4, periodIsOnline4,	periodMaxSpeed4, periodMinSpeed4 ,	periodAvgSpeed4,	"
            "periodOne5,	periodID5,	periodSpeedAlarmNum5,	periodAbnormalAlarmNum5, periodfatigueAlarm5,"
            "periodYawAlarmNum5, periodIsOnline5 ,	periodMaxSpeed5,	periodMinSpeed5,	periodAvgSpeed5,	"
            "periodOne6  ,	periodID6,	periodSpeedAlarmNum6,	periodAbnormalAlarmNum6, periodfatigueAlarm6,"
            "periodYawAlarmNum6,	periodIsOnline6,	periodMaxSpeed6,	periodMinSpeed6,	periodAvgSpeed6,	"
            "periodOne7, periodID7,	periodSpeedAlarmNum7,	periodAbnormalAlarmNum7,	periodfatigueAlarm7,"
            "periodYawAlarmNum7, periodIsOnline7,	periodMaxSpeed7, periodMinSpeed7,	periodAvgSpeed7,	"
            "periodOne8 ,periodID8,	periodSpeedAlarmNum8, periodAbnormalAlarmNum8,periodfatigueAlarm8,	"
            "periodYawAlarmNum8, periodIsOnline8, periodMaxSpeed8, periodMinSpeed8, periodAvgSpeed8) values ",
            insertResult, types_check=True)
    except Exception as e:
        logging.error(e)


def main():
    logging.info(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"))
    beginDetestr = "20191001"
    beginDate = datetime.datetime.strptime(beginDetestr, "%Y%m%d")
    endDetestr = "20191001"
    endDate = datetime.datetime.strptime(endDetestr, "%Y%m%d")
    cluster = connectCassendra()
    client = connectClickHouse()
    session = cluster.connect("track")
    allVehicleInfo = pd.DataFrame(list(session.execute(" select *  from vechile  ")))
    while beginDate <= endDate:
        allVehicleNos = list(
            session.execute(" select distinct license_plate from track_" + beginDetestr + " limit 1000 "))
        # 下面是将KeySlice转为标准的list
        vehicleNos = []
        for i in range(len(allVehicleNos)):
            vehicleNos.append(str(allVehicleNos[i][0]))
        insertResult = queryTrackByVehicleNo(session, vehicleNos, client, beginDetestr, allVehicleInfo)
        if insertResult:
            insertToClickhouse(client, insertResult)

        beginDate = beginDate + datetime.timedelta(days=1)
    session.shutdown()
    cluster.shutdown()
    logging.info(datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"))


if __name__ == '__main__':
    main()

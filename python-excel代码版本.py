# -*- coding: utf-8 -*-
"""
Created on Sat May 27 10:51:24 2017

@author: ZHAIXIAOFAN626
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 21 10:17:21 2017

@author: zxf-pc
"""
import pandas as pd
import numpy as np
import os
from datetime import datetime
from pandas.tseries.offsets import Day##########用于时间序列的偏移
class file_manage(object):
    #搜索文件,并保存文件路径名称
    def read_file(self,filedir):
        p=os.listdir(filedir)
        jihe=[]
        for i in range(len(p)):
            path=filedir+'/'+p[i]
            jihe.append(path)
        return jihe 
####################原始数据的准备，即为将csv格式转存为xlsx格式
    def Convert_table(self,filedir):
        csvjihe=application1.read_file(filedir)
        exceljihe=[]
        for i in range(len(csvjihe)):
            p=csvjihe[i].replace('.csv','.xlsx')
            exceljihe.append(p)
            path=filedir+'/'+csvjihe[i]
            newpath=filedir+'/'+p
            temp_file=pd.read_csv(path)
            temp_file.to_excel(newpath)
        return exceljihe
####################最终汇总函数(按列）)
    def Connect_table(self,filedir):
        p=os.listdir(filedir)
        jihe=[]
        for i in range(len(p)):
            path=filedir+'/'+p[i]
            l=pd.read_excel(path)
            jihe.append(l)
        connect_table=pd.concat(jihe)
        return connect_table
##################
    def Connect_tablehang(self,filedir):
        p=os.listdir(filedir)
        jihe=[]
        for i in range(len(p)):
            path=filedir+'/'+p[i]
            l=pd.read_excel(path)
            jihe.append(l)
        connect_table=pd.concat(jihe,axis=1)
        return connect_table    
            
##########复合索引下的索引重新定义
    def Finitial(self,path):    
        self.file=pd.read_excel(path)
        self.file.fillna(method='ffill')#将复合表的复合索引填充掉
###############不能轻易使用填充，会出现有意义空值被填充。
        df=pd.DataFrame(self.file).fillna(method='ffill')
        df.index=df.iloc[:,1]#重新定义索要的新索引
        time_value=df.iloc[:,3:]#取值，取得需要的值并去除复合索引
        return time_value,self.file
##########不带复合索引下的文件初始化
    def initial(self,path):    
        self.file=pd.read_excel(path)
        df=pd.DataFrame(self.file)
        return df
###########不进行求和部分的创立新表   
    def create_tablensum(self,time_value):
        l=[]
#使用pandas的时间序列处理，但是只能针对Series
        for i in range(len(time_value.iloc[:,0])):
#一周为周期的时间序列的求和
            a=pd.DataFrame(time_value.iloc[i,:].resample('7D',limit='7D').sum()).T###注意时间
            l.append(a)
        table=pd.concat(l)        
        return table    
############创建含有复合索引的新表格        
    def create_table(self,time_value,file):
        l=[]
#使用pandas的时间序列处理，但是只能针对Series
        for i in range(len(time_value.iloc[:,0])):
#一周为周期的时间序列的求和
            a=pd.DataFrame(time_value.iloc[i,:].resample('7D').sum()).T###注意时间
            l.append(a)
########重新组合新表格,去掉不需要的列(根据要求只保留时间项)，包含了自定义的总计
        table=pd.concat(l)
        matrix=np.matrix(table)
        a,b=matrix.shape
        new_matrix=np.zeros((a+1,b))
        new_matrix[:-1]=matrix
        new_matrix[-1]=matrix.sum(axis=0)
########创建符合要求的新表格
        c=list(file['产品子类'])
        c.append('总计')
        frame=pd.DataFrame(new_matrix,index=[['存','存','投资','投资','投资','保险','保险','总计'],c],columns=table.columns)
        frame.to_excel('/Users/zxf-pc/Desktop/平安金科/实验文档1.xlsx')
        return frame
##########经过以上处理得到的是我所想要的单个标准化表格，最总表格需要按要求求和得到
#########得到月份的日报内容
    #def Ribao_chuli(self,filedir):
##############################################构建符合index功能
    def Make_index(self,word):
        self.name_list=[]
        for i in range(5):
            self.name_list.append(word)
        return self.name_list
######################################################
    def HeBing_table(self,filedir):
        #################################所有结果的合并
        #filedir='D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/完整表格'
        zuizong_table=application1.Connect_table(filedir)######在主函数中定义了application1=file_manage
        tzuizong_table=[]
###########################按要求以最新日期开头,即为前后颠倒
        for i in range(len(zuizong_table.iloc[0,:])):
            tzuizong_table.append(zuizong_table.iloc[:,-(i+1)])
        newzuizong_table=pd.DataFrame(tzuizong_table).T
#########################DataFrame的切片为Series，而Series是列形式，没有columns
        danwei=newzuizong_table.pop('单位')
        newzuizong_table.insert(0,'单位',danwei)
        newzuizong_table.to_excel('D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/完整表格按周/最终结果.xlsx')
#########################csv格式的副本
        newzuizong_table.to_csv('D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/完整表格按周/最终结果副本.csv')
        return newzuizong_table
if __name__=='__main__':
    try:
#####################处理日活部分（周部分）
        appfiledir='D:/Users/ZHAIXIAOFAN626/Desktop/数据/准备文件/4月26日更新/公司总体/app日活'
        appfiledir2='D:/Users/ZHAIXIAOFAN626/Desktop/数据/准备文件/4月26日更新/一账通日活处理'
        application1=file_manage()
        app_yizhangtong=application1.Connect_tablehang(appfiledir2)
        app_yizhangtong.to_excel(appfiledir+'/'+'一账通app日活.xlsx')
        #application1.read_file(appfiledir)
        appjihe=application1.read_file(appfiledir)
##################################5月11日修改部分
        appmean_time=pd.date_range('2017-1-18',periods=21,freq='7D')#######################此处到5月31号
###############################################################
        temp=[]
        for i in range(len(appjihe)):
            df1=application1.initial(appjihe[i])
            #new_table=application1.create_tablensum(df1)###############
            temp.append(df1)
            #temp.append(new_table)
        apptemp1zong_biao=pd.concat(temp)
        #apptemp1zong_biao.to_excel('D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/app日活/app日活1.xlsx')
        appheji_biaoge=apptemp1zong_biao.sum(axis=0)
        apptemp2zong_biao=pd.DataFrame(appheji_biaoge).T
        #appsum=0
        #Mouth=[]
        mean_list=[]
        #app_time_list=[]
        mean_value=[]########################目标日期下的数值
        for l in range(1,7):#################调整月份处，月份变化时，一定要调整
            Mouth=[]
            #appsum=0
            p=[]
            app_time_list=[]
            app_date_number=[]
            #slice0=[]
###############################################################取出目标积累日期，5月11日修改部分,目前仍是单节月份，遇到双字节月份要调整
###############考虑双月份问题，只要再加一个if即可
            for g in range(len(appmean_time)):
                if str(appmean_time[g])[6]==str(l):
                    app_time_list.append(appmean_time[g])
                    #print('adding successful')
            for m in range(len(app_time_list)):
                app_date_number.append(int(str(app_time_list[m])[8:10]))
            #print(app_date_number)
#############################################################取出单独月份部分
            for i in range(len(apptemp2zong_biao.columns)):
                if str(apptemp2zong_biao.columns[i])[6]==str(l):##################当月份超过10之后，在另行修改
                    p.append(i)
                    a=p[0]
                    b=p[-1]
                    slice0=pd.DataFrame(apptemp2zong_biao.iloc[0,a:b+1]).T
            Mouth.append(slice0)
########################################################5月11日修改部分
########################################################获得指定日期下的值
            for h in range(len(app_date_number)):
                app_title=app_time_list[h]
                app_temp_number=slice0.ix[:,:slice0[app_title].name].sum(axis=1).values/app_date_number[h]
                mean_value.append(app_temp_number)
############################################################5月11日修改前算法
            #for h in range(len(slice0.columns)):
                #appsum=appsum+slice0.iloc[:,h].values              
                #mean_values=appsum/(70000*(h+1))################此处的月均算法有问题
                #date=int(str(slice0.columns[h])[8:10])#######################5月9号修改部分
                #if date <=25:
                 #   date_number=(int(str(slice0.columns[h])[8:10])+6)*10000
                    #mean_values=appsum/date_number
                #else:
                 #   date_number=31*10000
                #mean_values=appsum/date_number ####################5月9号修改部分   
                #Mouth.append(mean_values)
                #slice0.iloc[:,h]=mean_values
            #mean_list.append(slice0)
#####################################################################5月11日前修改
        appmean_temp_number=np.matrix(mean_value)/10000
        appmean_list=pd.DataFrame(appmean_temp_number).T
        appmean_list.columns=appmean_time
        #appmean_list=pd.concat(mean_list,axis=1)
        appmean_list.index=[['公司总体'],['APP日活']]
#############################限定时间日期部分，要改动,为了使表格一至
########################################################################5月11日修改部分
        #appmean_list.columns=pd.date_range('2017-1-18',periods=17,freq='7D')################需要根据最后日期增加period的值
#######################################################################################        
        appmean_list.insert(0,'单位',['万'])
        appmean_list.to_excel('D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/完整表格/公司总体app日活.xlsx')
#############检验部分
        #zong_biao.to_excel('D:/Users/ZHAIXIAOFAN626/Desktop/数据/准备文件/4月/app日活/shiyan1.xlsx')
#############处理日活模块

#############处理不过主账户模块(周模式)，不完整少数据
        buguozhu_filedir='D:/Users/ZHAIXIAOFAN626/Desktop/数据/准备文件/4月25日更新/不过主账户'
        buguozhujihe=application1.read_file(buguozhu_filedir)
        temp1=[]
        buguozhu_jibu_time=pd.date_range('2017-1-11',periods=22,freq='7D')######################注意修改时间
        for i in range(len(buguozhujihe)):
             df2=application1.initial(buguozhujihe[i])
             buguo_insert_file=df2.pop('银行类型')
             buguo_index=df2.pop('产品名称')
             df2.fillna(0,inplace=True)####################inplace=True直接替换原标，不是产生新的对象
########################################################最初保留部分
      #      df2.index=df2['银行类型']
       #     del df2['银行类型']
########################################################最初保留部分
             new_table1=application1.create_tablensum(df2)/100000000
        buguo_zhu_sum=new_table1.iloc[:,0]
        for l in range(1,len(new_table1.columns)):
            buguo_zhu_sum=buguo_zhu_sum+new_table1.iloc[:,l]
            new_table1.iloc[:,l]=buguo_zhu_sum    
        new_table1.index=buguo_insert_file
        new_table1.columns=buguozhu_jibu_time
        new_table1.insert(0,'单位',['亿元','亿元','亿元','亿元','亿元','亿元'])
        new_table1.insert(0,'产品名称',buguo_index.values)
###############################################################################最初保留部分
        #new_table1.columns=buguozhu_jibu_time     
         #   temp1.append(new_table1)
        #buguozhutemp1zong_biao=pd.concat(temp1)
        #buguozhutemp1zong_biao.loc['累计销售额']=buguozhutemp1zong_biao.sum(axis=0)
       # buguotemp2zong_biao=np.matrix(pd.DataFrame(buguozhutemp1zong_biao.loc['累计销售额']).T)
        #buguozhu_table=pd.DataFrame(buguotemp2zong_biao,index=[['不过主账户'],['累计销售额']])
        #buguozhu_table.columns=buguozhutemp1zong_biao.columns
############################################################################最初保留部分
        new_table1.to_excel('D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/不过主账户/不过主账户.xlsx')
#############APP注册用户数##################5月25日新增

################处理AUM模块(周处理)
        AUM_filedir='D:/Users/ZHAIXIAOFAN626/Desktop/数据/准备文件/4月25日更新/公司总体/Aum/全部AUM.xlsx'
        AUMjihe=pd.read_excel(AUM_filedir)
        del AUMjihe['渠道']
        del AUMjihe['渠道子类']
        AUm_temp=AUMjihe.iloc[-1,:]################DataFrame的silce是Series
        strtime_range=[]
        index=AUm_temp.index
        time_index=[]
        for i in range(len(index)):
            time_index.append(str(index[i]))
        for i in range(len(time_index)):
            year=time_index[i][:4]
            if time_index[i][-2]=='0':
                mouth=time_index[i][4:].replace('0','-')
                date=year+mouth
                newdate=datetime.strptime(date,'%Y-%m-%d')
                strtime_range.append(newdate)
            else:
                new_str='-'+time_index[i][-3]+'-'+time_index[i][-2:]
                new_date=time_index[i].replace(time_index[i][4:],new_str)
                new_date1=datetime.strptime(new_date,'%Y-%m-%d')
                strtime_range.append(new_date1)
        #time=pd.date_range('2017-4-1','2017-4-19',freq='D')
        #time=pd.date_range('2017-4-1','2017-4-24',freq='D')
        #AUm_temp.index=time
        AUm_temp.index=strtime_range
        Aum_temp2=AUm_temp.resample('7D').sum()#####################
        for i in range(len(Aum_temp2.index)):
            #now=Aum_temp2.index[i]
            #new_now=now+6*Day()
            Aum_temp2.loc[Aum_temp2.index[i]]=AUm_temp.loc[Aum_temp2.index[i]]######AUM值为最后累计值
        AUM_table1=np.matrix(pd.DataFrame(Aum_temp2).T)/100000000
        AUM_table2=pd.DataFrame(Aum_temp2).T
        AUM_table=pd.DataFrame(AUM_table1,index=[['公司总体'],['AUM']],columns=AUM_table2.columns)
        AUM_table.insert(0,'单位',['亿元'])
        AUM_table.to_excel('D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/完整表格/公司总体AUM.xlsx')
######################处理AUM

#########################处理注册用户数（按周来）,数据需要加上之前的累计值1/11的144.45
        zhuce_filedir='D:/Users/ZHAIXIAOFAN626/Desktop/数据/准备文件/4月25日更新/公司总体/注册用户数'
        zhucejihe=application1.read_file(zhuce_filedir)
        temp2=[]
        for i in range(len(zhucejihe)):
            df3=application1.initial(zhucejihe[i])
            new_table2=application1.create_tablensum(df3)/10000
            temp2.append(new_table2)
        zhuce_table1=pd.concat(temp2)
        zhuce_table=pd.DataFrame(zhuce_table1.sum(axis=0)).T
#################################################################加上累计数值144.45
        temp_sum=144.45
        for i in range(len(zhuce_table.columns)):
            new_temp_sum=zhuce_table.iloc[:,i]+temp_sum
            zhuce_table.iloc[:,i]=new_temp_sum
            temp_sum=new_temp_sum
#####################################################################
        zhuce_table.index=[['公司总体'],['注册用户数']]
##################三层索引类似zhuce_table.index=[[A],[B],[C]]
        #zhuce_table.insert(0,'2017/1/11',144.45)
        jilei_time=pd.date_range('2017-1-18',periods=21,freq='7D')#####日期为积累量，需要定期调整，后面通用
        zhuce_table.columns=jilei_time#############
        zhuce_table.insert(0,'2017-1-11',144.45)
        zhuce_table.insert(0,'单位',['万'])
        zhuce_table.to_excel('D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/完整表格/公司总体注册用户数.xlsx')
###################周注册用户

####################开户用户数，有问题,先对银行一账通开户作调整
        kaihu_filedir='D:/Users/ZHAIXIAOFAN626/Desktop/数据/准备文件/4月25日更新/公司总体/开户用户数'
        yinhang_filedir='D:/Users/ZHAIXIAOFAN626/Desktop/数据/准备文件/4月25日更新/公司总体/银行一账通开户用户数处理/银行一账通开户用户数.xlsx'
        yinhang_file=pd.read_excel(yinhang_filedir)
        yinhang_index=yinhang_file.pop('用户类型')
        yinhang_file.index=yinhang_index
        yinhang_file2=yinhang_file.sum(axis=0)########变为一个Series
        yinhang_file3=yinhang_file2.resample('7D').sum()
        yinhang_kaihu=pd.DataFrame(yinhang_file3).T
        yinhang_kaihu.columns=jilei_time
###########################################################直接在此处加上开户累积量
        yinhang_kaihu_sum=333300-184526
        for g in range(len(yinhang_kaihu.columns)):
            yinhang_kaihu.iloc[:,g]=yinhang_kaihu_sum+yinhang_kaihu.iloc[:,g]
            yinhang_kaihu_sum=yinhang_kaihu.iloc[:,g]
            
        #yinhang_index=yinhang_kaihu.pop('用户类型')
        #yinhang_kaihu.index=yinhang_index.sum()
#########################################Index的形式必须是list
        yinhang_kaihu.index=['网银加直销']
########################################Index的名为字符形式,开户用户数的计算有问题
        yinhang_kaihu.index.name='用户类型'
        yinhang_kaihu.to_excel(kaihu_filedir+'/'+'银行一账通开户用户数.xlsx')
###################对银行一账通的开户用户进行处理，使其成为刻度文件形式，且时间相同
        #kaihu_filedir='D:/Users/ZHAIXIAOFAN626/Desktop/数据/准备文件/4月25日更新/公司总体/开户用户数'
        kaihujihe=application1.read_file(kaihu_filedir)
        temp3=[]
        for i in range(len(kaihujihe)):
            df4=pd.read_excel(kaihujihe[i],index_col='用户类型')
            new_table3=application1.create_tablensum(df4)/10000
            temp3.append(new_table3)
        kaihu_table1=pd.concat(temp3)
        kaihu_table=pd.DataFrame(kaihu_table1.sum(axis=0)).T##############此处求和有问题
        
        
########################################这个积累时间有问题
        #kaihu_table.columns=jilei_time
##################时间修改为积累时间
        kaihu_table.index=[['公司总体'],['用户开户数']]
        kaihu_table.insert(0,'单位',['万'])
        kaihu_table.to_excel('D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/完整表格/公司总体开户用户数.xlsx')
##########################主账户部分
        zhuzhanghu_path='D:/Users/ZHAIXIAOFAN626/Desktop/数据/准备文件/4月25日更新/主账户/5月3日'
        zhuzhanghu_jihe=application1.read_file(zhuzhanghu_path)
        zhuzhanghu_list=[]
        for i in range(len(zhuzhanghu_jihe)):
            df5=pd.read_excel(zhuzhanghu_jihe[i])
            zhuzhanghu_list.append(df5)
        zhuzhanghu_table=pd.concat(zhuzhanghu_list)
        zhuzhanghu_table.iloc[0,:]=zhuzhanghu_table.iloc[0,:]/10000
        zhuzhanghu_table.iloc[1,:]=zhuzhanghu_table.iloc[1,:]/10000
        zhuzhanghu_table.iloc[2,:]=zhuzhanghu_table.iloc[2,:]/100000000
        zhuzhanghu_table.iloc[3,:]=zhuzhanghu_table.iloc[3,:]/10000
        zhuzhanghu_table.iloc[4,:]=zhuzhanghu_table.iloc[4,:]/100000000
        zhuzhanghu_table.index=[['主账户','主账户','主账户','主账户','主账户'],zhuzhanghu_table.index]
        zhuzhanghu_table.insert(0,'单位',['万','万','亿元','万','亿元'])
        #zhuzhanghu_table.index.name=[['产品类别'],['维度']]
        zhuzhanghu_table.to_excel('D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/完整表格/主账户.xlsx')                      
############################过主账户销售情况
        guozhu_path='D:/Users/ZHAIXIAOFAN626/Desktop/数据/准备文件/4月25日更新/过主账户销售情况' 
        guozhu_jihe=application1.read_file(guozhu_path)
        guozhu_table1=[]
        for i in range(len(guozhu_jihe)):
            df6=pd.read_excel(guozhu_jihe[i],index_col='产品子类')
            Index_one=df6.pop('产品大类 ')
            guozhu_table1.append(df6.T)
        guozhu_table=pd.concat(guozhu_table1).T/100000000
########################################################创建新表格
        guozhu_file=[]
        cun_file=pd.DataFrame([guozhu_table.loc['货基'],guozhu_table.loc['活期']])   
        guozhu_file.append(cun_file)
        tou_file=pd.DataFrame([guozhu_table.loc['互联网产品'],guozhu_table.loc['一行三会产品'],guozhu_table.loc['基金（非货基）'],guozhu_table.loc['定期']])
        guozhu_file.append(tou_file)
        bao_file=pd.DataFrame([guozhu_table.loc['意外险'],guozhu_table.loc['重疾险']])
        guozhu_file.append(bao_file)
        xiaofei_file=pd.DataFrame([guozhu_table.loc['信用卡还款'],guozhu_table.loc['生活缴费'],guozhu_table.loc['实物金'],guozhu_table.loc['其他']])
        guozhu_file.append(xiaofei_file)
        dai_file=pd.DataFrame([guozhu_table.loc['应急钱包'],guozhu_table.loc['卡优贷'],guozhu_table.loc['小贷平台']])
        guozhu_file.append(dai_file)
        guozhu_zuizong=pd.concat(guozhu_file)
        guozhu_zuizong.loc['合计']=guozhu_zuizong.sum(axis=0)
        Index_two=['存','存','投','投','投','投','保','保','消费','消费','消费','消费','贷','贷','贷','合计']
        guozhu_zuizong.index=[Index_two,guozhu_zuizong.index]
        Index_three=[]
        for i in range(16):
            Index_three.append('亿元')
        guozhu_zuizong.insert(0,'单位',Index_three)
#######################################
        zhu_zuizong_table=[]
###########################按要求以最新日期开头,即为前后颠倒
        for i in range(len(guozhu_zuizong.iloc[0,:])):
            zhu_zuizong_table.append(guozhu_zuizong.iloc[:,-(i+1)])
        newzhuzuizong_table=pd.DataFrame(zhu_zuizong_table).T
        #guozhu_zuizong.to_excel('D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/过主账户销售/过主账户销售.xlsx')
        danwei=newzhuzuizong_table.pop('单位')
        newzhuzuizong_table.insert(0,'单位',danwei)
        newzhuzuizong_table.to_excel('D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/过主账户销售/过主账户销售.xlsx')
#########################分渠道销售部分
        fenqudao_filedir='D:/Users/ZHAIXIAOFAN626/Desktop/数据/准备文件/4月25日更新/分渠道过主账户销售情况'          
        fenqudaojihe=application1.read_file(fenqudao_filedir)
        fenqudao_yizhangtong=[]
        fenqudao_yinhangyizhangtong=[]
        fenqudao_xiaodaipingtai=[]
        fenqudao_yizhangtong_index=[]
        for h in range(5):
            fenqudao_yizhangtong_index.append('一账通')
############################注意时间变更时，要认为进行改动
        fenqudao_time=pd.date_range('2017-1-11',periods=19,freq='7D')###############时间也是要调整的
        for i in range(len(fenqudaojihe)):
            df7=pd.read_excel(fenqudaojihe[i],index_col='渠道')
            df7.pop('产品大类 ')
            df7.pop('产品子类')
            df7.pop('交易笔数')
            df7.dropna(how='all')
            fenqudao_yizhangtong.append(df7.loc['一账通'])
            fenqudao_yinhangyizhangtong.append(df7.loc['银行一账通'])
            fenqudao_xiaodaipingtai.append(df7.loc['小贷平台'])
############################一账通部分
        fenqudao_yizhangtong0=pd.DataFrame(fenqudao_yizhangtong)
        fenqudao_yizhangtong0.pop('子渠道')
        fenqudao_yizhangtong0.index=fenqudao_time
        fenqudao_yizhangtong1=fenqudao_yizhangtong0.T
################################################################建立规范表格部分
        fenqudao_yizhangtong1.iloc[0,:]=fenqudao_yizhangtong1.iloc[0,:]/10000
        fenqudao_yizhangtong1.iloc[1,:]=fenqudao_yizhangtong1.iloc[1,:]/10000
        fenqudao_yizhangtong1.iloc[2,:]=fenqudao_yizhangtong1.iloc[2,:]/100000000
        fenqudao_yizhangtong1.iloc[3,:]=fenqudao_yizhangtong1.iloc[3,:]/10000
        fenqudao_yizhangtong1.iloc[4,:]=fenqudao_yizhangtong1.iloc[4,:]/100000000
        fenqudao_yizhangtong1.insert(0,'单位',['万','万','亿元','万','亿元'])
        fenqudao_yizhangtong1.index=[fenqudao_yizhangtong_index,fenqudao_yizhangtong_index,fenqudao_yizhangtong1.index]
        fenqudao_yizhangtong1.to_excel('D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/分渠道销售情况局部处理/一账通分渠道销售.xlsx')
##################################银行一账通部分
        fenqudao_yinhang_zong0=[]
        for i in range(len(fenqudao_yinhangyizhangtong)):
            yinhangyizhang_tong=pd.DataFrame(fenqudao_yinhangyizhangtong[i])
            yinhang_index0=yinhangyizhang_tong.pop('子渠道')
            yinhangyizhang_tong.index=yinhang_index0
            jubu_time=fenqudao_time[i:1+i]
            #fenqudao_yinhang_zong0.append(yinhangyizhang_tong)
            fenqudao_list=yinhangyizhang_tong.index.drop_duplicates()
            fenqudao_jubu_index=[]
            fenqudao_jubu_zong0=[]
            fenqudao_jubu_zong1=[]
        #fenqudao_yinhangyizhangtong_zong=pd.concat(fenqudao_yinhang_zong0)
            for j in range(len(fenqudao_list)):
                if len( yinhangyizhang_tong.loc[fenqudao_list[j]])==5 and fenqudao_list[j]!='上海银行':#发现上海银行还有冲突特别处理一下
                    fenqudao_yinhangyizhangtong_file=pd.DataFrame(yinhangyizhang_tong.loc[fenqudao_list[j]])
                    fenqudao_yinhangyizhangtong_file.columns=jubu_time
                    fenqudao_yinhangyizhangtong_file.iloc[0,:]=fenqudao_yinhangyizhangtong_file.iloc[0,:]/10000
                    fenqudao_yinhangyizhangtong_file.iloc[1,:]=fenqudao_yinhangyizhangtong_file.iloc[1,:]/10000
                    fenqudao_yinhangyizhangtong_file.iloc[2,:]=fenqudao_yinhangyizhangtong_file.iloc[2,:]/100000000
                    fenqudao_yinhangyizhangtong_file.iloc[3,:]=fenqudao_yinhangyizhangtong_file.iloc[3,:]/10000
                    fenqudao_yinhangyizhangtong_file.iloc[4,:]=fenqudao_yinhangyizhangtong_file.iloc[4,:]/100000000
                    fenqudao_yinhangyizhangtong_file.insert(0,'单位',['万','万','亿元','万','亿元'])
                    for l in range(5):
                        fenqudao_jubu_index.append(fenqudao_list[j])
                else:    
                    fenqudao_yinhangyizhangtong_file=pd.DataFrame(yinhangyizhang_tong.loc[fenqudao_list[j]].sum(axis=0))
                    fenqudao_yinhangyizhangtong_file.columns=jubu_time
                    fenqudao_yinhangyizhangtong_file.iloc[0,:]=fenqudao_yinhangyizhangtong_file.iloc[0,:]/10000
                    fenqudao_yinhangyizhangtong_file.iloc[1,:]=fenqudao_yinhangyizhangtong_file.iloc[1,:]/10000
                    fenqudao_yinhangyizhangtong_file.iloc[2,:]=fenqudao_yinhangyizhangtong_file.iloc[2,:]/100000000
                    fenqudao_yinhangyizhangtong_file.iloc[3,:]=fenqudao_yinhangyizhangtong_file.iloc[3,:]/10000
                    fenqudao_yinhangyizhangtong_file.iloc[4,:]=fenqudao_yinhangyizhangtong_file.iloc[4,:]/100000000
                    fenqudao_yinhangyizhangtong_file.insert(0,'单位',['万','万','亿元','万','亿元'])
                    for h in range(5):
                        fenqudao_jubu_index.append(fenqudao_list[j])
                fenqudao_yinhangyizhangtong_file.index=[fenqudao_jubu_index,fenqudao_yinhangyizhangtong_file.index]
########################################################这里还缺少一个插入单位部分                
                fenqudao_jubu_index=[]
                fenqudao_jubu_zong0.append(fenqudao_yinhangyizhangtong_file)
            fenqudao_jubu_zong1=pd.concat(fenqudao_jubu_zong0)
            fenqudao_yinhang_zong0.append(fenqudao_jubu_zong1)
        fenqudao_yinhang_zuizong=pd.concat(fenqudao_yinhang_zong0,axis=1)
        fenqudao_yinhang_zuizong.drop('合计')
########################################################5月15日修改
        fenqudao_yinhang_zuizong.pop('单位')
        fenqudao_yinhang_zuizong=fenqudao_yinhang_zuizong.fillna(0)
########################################################5月15日修改
#######################################################5月27日改了需求
        Azu_list=[]
        Bzu_list=[]
        Azu=['紫金农商','启东农商','苏州银行','吉林银行','南和农商','威海银行','阜新银行']
        Bzu=['上海银行','华瑞银行','佛山农商','贵阳银行','绵阳银行','平安银行','顺德农商','重庆银行','自贡银行']
        Azu_zong=fenqudao_yinhang_zuizong.loc['紫金农商']
        Azu_list.append(Azu_zong)
        for i in range(1,len(Azu)):
            Azu_zong=Azu_zong+fenqudao_yinhang_zuizong.loc[Azu[i]]
            Azu_list.append(fenqudao_yinhang_zuizong.loc[Azu[i]])
        Azu_list.append(Azu_zong)
        Bzu_zong=fenqudao_yinhang_zuizong.loc['上海银行']
        Bzu_list.append(Bzu_zong)
        for j in range(1,len(Bzu)):
            Bzu_zong=Bzu_zong+fenqudao_yinhang_zuizong.loc[Bzu[j]]
            Bzu_list.append(fenqudao_yinhang_zuizong.loc[Bzu[j]])
        Bzu_list.append(Bzu_zong)
       # beiqu_zong=fenqudao_yinhang_zuizong.loc['阜新银行']
        #beiqu_list.append(beiqu_zong)
        #for h in range(1,len(beiqu)):
        #    beiqu_zong=beiqu_zong+fenqudao_yinhang_zuizong.loc[beiqu[h]]
        #    beiqu_list.append(fenqudao_yinhang_zuizong.loc[beiqu[h]])
        #beiqu_list.append(beiqu_zong)
###########################################################合并表格部分
        Azu_zui_zong=pd.concat(Azu_list)
        Azu_index=[]
        Bzu_index=[]
        #beiqu_index=[]
        suoyou_Azu_index=['A组小计','A组小计','A组小计','A组小计','A组小计']
        suoyou_Bzu_index=['B组小计','B组小计','B组小计','B组小计','B组小计']
        #suoyou_beiqu_index=['北区小计','北区小计','北区小计','北区小计','北区小计']
        suoyou_heji_index=['银行一账通合计','银行一账通合计','银行一账通合计','银行一账通合计','银行一账通合计']
        for i in range(len(Azu)):
            temp_name_list=application1.Make_index(Azu[i])
            Azu_index=Azu_index+temp_name_list
        Azu_index=Azu_index+suoyou_Azu_index
        Azu_zui_zong.index=[Azu_index,Azu_zui_zong.index]
        Bzu_zui_zong=pd.concat(Bzu_list)
        for j in range(len(Bzu)):
            temp_name_list1=application1.Make_index(Bzu[j])
            Bzu_index=Bzu_index+temp_name_list1
        Bzu_index=Bzu_index+suoyou_Bzu_index
        Bzu_zui_zong.index=[Bzu_index,Bzu_zui_zong.index]
        quanqu_zui_zong=pd.concat([Azu_zui_zong,Bzu_zui_zong])
#############################################################最后的合计部分
        zui_zong_heji=quanqu_zui_zong.loc['A组小计']+quanqu_zui_zong.loc['B组小计']  
        zui_zong_heji.index=[suoyou_heji_index,zui_zong_heji.index]
        quanqu_zui_zong0=pd.concat([Azu_zui_zong,Bzu_zui_zong,zui_zong_heji])
########################################################5月15号修改部分
        #fenqudao_yinhang_zuizong.to_excel('D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/分渠道销售情况局部处理/银行一账通分渠道.xlsx')
        quanqu_zui_zong0.to_excel('D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/分渠道销售情况局部处理/银行一账通分渠道.xlsx')
###########################################小贷平台部分
        fenqudao_xiaodai_zong0=[]
        fenqudao_xiaodai_index=[]
        for h in range(5):
            fenqudao_xiaodai_index.append('小贷平台')
        #fenqudao_xiaodai_zong1=[]
        #fenqudao_xiaodaipingtai.pop('子渠道')
        for i in range(len(fenqudao_xiaodaipingtai)):
            xiaodai_file=fenqudao_xiaodaipingtai[i]
            xiaodai_file.pop('子渠道')
            xiaodai_pingtai=pd.DataFrame(xiaodai_file.sum(axis=0)/2)
            xiaodai_pingtai.columns=fenqudao_time[i:i+1]
            xiaodai_pingtai.iloc[0,:]=xiaodai_pingtai.iloc[0,:]/10000
            xiaodai_pingtai.iloc[1,:]=xiaodai_pingtai.iloc[1,:]/10000
            xiaodai_pingtai.iloc[2,:]=xiaodai_pingtai.iloc[2,:]/100000000
            xiaodai_pingtai.iloc[3,:]=xiaodai_pingtai.iloc[3,:]/10000
            xiaodai_pingtai.iloc[4,:]=xiaodai_pingtai.iloc[4,:]/100000000
            #xiaodai_pingtai.insert(0,'单位',['万','万','亿元','万','亿元'])
            xiaodai_pingtai.index=[fenqudao_xiaodai_index,xiaodai_pingtai.index]
            fenqudao_xiaodai_zong0.append(xiaodai_pingtai)
        fenqudao_xiaodai_zuizong=pd.concat(fenqudao_xiaodai_zong0,axis=1)
####################################6月1日修改
        #fenqudao_xiaodai_zuizong.insert(0,'单位',['万','万','亿元','万','亿元'])
##########################################6月1日修改
        #fenqudao_xiaodao_zuizong.cloumns=fenqudao_time
        fenqudao_xiaodai_zuizong.to_excel('D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/分渠道销售情况局部处理/小贷平台.xlsx')
###############################################分渠道合并文件部分，有问题，合并目前使用手动。
#########################################6月1号修改
        file=fenqudao_yizhangtong1.loc['一账通'].loc['一账通']
        file2=fenqudao_yizhangtong1.loc['一账通']
        file2.pop('单位')
        file.pop('单位')
        fenqudao_zuizong_heji_index=['合计','合计','合计','合计','合计']
        fenqudao_zuizong_heji=zui_zong_heji.loc['银行一账通合计']+fenqudao_xiaodai_zuizong.loc['小贷平台']+file
        fenqudao_zuizong_heji.index=[fenqudao_zuizong_heji_index,fenqudao_zuizong_heji.index]
        fenqudao_zuizong_heji1=pd.concat([file2,quanqu_zui_zong0,fenqudao_xiaodai_zuizong,fenqudao_zuizong_heji])
        fenqudao_zuizong_heji1.to_excel('D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/分渠道销售情况局部处理/合计.xlsx')      
            
            
        
                
            
            

    
            
            
            
            



##################################过主账户模块（以周记）
        #guozhu_filedir=''
#################################所有结果的合并
        filedir='D:/Users/ZHAIXIAOFAN626/Desktop/数据/预处理完成文件/完整表格'
        newzuizong_table=application1.HeBing_table(filedir)
    
    
    
    
    
    
    
    except Exception as e:
        print('Error,生成文件未删除')
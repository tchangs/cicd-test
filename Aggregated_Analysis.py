# Databricks notebook source
# DBTITLE 1,Import Libraries
import pyspark
import numpy as np
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import time
import os
import sys
import traceback
import math
import pytz
from pyspark.sql import *
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, lit, when
from functools import reduce
from datetime import datetime, timezone
import warnings
warnings.filterwarnings("ignore")

dbutils.widgets.removeAll()


# COMMAND ----------

dbutils.widgets.dropdown("1.Customers", "GHS", ["GHS", "STORMV", "WAKE", "KUMED"])
dbutils.widgets.text("2.StartDate", "2019-06-01")
dbutils.widgets.text("3.EndDate", "2019-09-01")
dbutils.widgets.dropdown("5.Generic_Name", "propofol", ["All", "propofol"])

# COMMAND ----------

customer = dbutils.widgets.get("1.Customers")
startdate = dbutils.widgets.get("2.StartDate")
enddate = dbutils.widgets.get("3.EndDate")
genericname = dbutils.widgets.get("5.Generic_Name")
print("Customer = {}, StartDate = {}, EndDate = {}, Generic_Name = {}".format(customer, startdate, enddate, genericname))

# COMMAND ----------

# DBTITLE 1,Get global_item_master
sql = """
  select * 
  from itemmasterproduction.global_item_master 
  where  lower(generic_name) like '{}%' 
  order by generic_name, global_item_id
  """.format(genericname)
global_itemmaster_df = sqlContext.sql(sql)

# COMMAND ----------

display(global_itemmaster_df)

# COMMAND ----------

# DBTITLE 1,Register as temp table
global_itemmaster_df.createOrReplaceTempView("vw_global_item_master")

# COMMAND ----------

sql = """
    select t2.global_item_id,t2.trade_identifier as NDC
    from vw_global_item_master t1
    left join itemmasterproduction.global_item_master_trade_items t2 on (trim(t1.global_item_id) = trim(t2.global_item_id))
    order by global_item_id
  """
item_ndc_df = sqlContext.sql(sql)

# COMMAND ----------

display(item_ndc_df)

# COMMAND ----------

# DBTITLE 1,Register as temp table
item_ndc_df.createOrReplaceTempView("vw_item_ndc")

# COMMAND ----------

sql = """
  select distinct t1.global_item_id,t1.NDC, t2.cpckey, t2.item_id, t2.item_name
   from vw_item_ndc t1
     left join ds_development.rk_barcode_ndc_map_0409 t2 on (trim(t1.NDC) = trim(t2.ndc))
   where t2.ndc is not null
   order by cpckey,global_item_id
  """
items_df = sqlContext.sql(sql)

# COMMAND ----------

# DBTITLE 1,Register as temp table
items_df.createOrReplaceTempView("vw_items")

# COMMAND ----------

display(items_df)

# COMMAND ----------

# DBTITLE 1,Gather some stats from the analysis so far
# MAGIC %sql
# MAGIC select "Number of Customers with Propofol" as desc_, count(distinct cpckey) as n from vw_items
# MAGIC union all
# MAGIC select "Number of Propofol types in GF" as desc_, count(distinct global_item_id) as n from vw_global_item_master
# MAGIC union all
# MAGIC select "Number of Propofol types in CF" as desc_, count(distinct global_item_id) as n from vw_items
# MAGIC union all
# MAGIC select "Number of Propofol NDCs in GF" as desc_, count(distinct NDC) as n from vw_item_ndc
# MAGIC union all
# MAGIC select "Number of Propofol NDCs in CF" as desc_, count(distinct NDC) as n from vw_items

# COMMAND ----------

# DBTITLE 1,Add performance score of cpc , this will help us decide whether to include the cpc or not
sql = """
  select distinct t1.global_item_id,t1.cpckey,t1.item_id, t2.perf_score
  from vw_items t1
    left join ds_development.rk_barcode_ndc_map_customer_score_0409 t2 on (trim(t1.cpckey) = trim(t2.cpckey))
  order by cpckey,global_item_id
"""
items_customer_score_df = sqlContext.sql(sql)

# COMMAND ----------

items_customer_score_df.createOrReplaceTempView("vw_items_customer_score")

# COMMAND ----------

display(items_customer_score_df)

# COMMAND ----------

# DBTITLE 1,Add the stocking area Ids
sql = """
  select distinct t1.* , trim(t2.omni_stid) as omni_stid
  from vw_items_customer_score t1
    left join ds_development.ocautodata_itemlocation t2 on 
    (trim(t1.cpckey) = trim(t2.cpckey) and 
    trim(t1.item_id) = trim(t2.item_id))
  order by cpckey,omni_stid,global_item_id
  """
items_loc_df = sqlContext.sql(sql)
items_loc_df.createOrReplaceTempView("vw_items_loc")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_items_loc where omni_stid is null

# COMMAND ----------

sql = """
  select CustomerId,cpckey, 
         min(perf_score) as score_min, max(perf_score) as score_max,
         count(distinct omni_stid) as n_exp, 
         count(distinct StockAreaId) as n_obs
  from (select t1.*, t2.* 
        from (select distinct cpckey,omni_stid,item_id, perf_score 
              from vw_items_loc where omni_stid is not null) t1
        left join (select distinct CustomerId,StockAreaId,ItemId 
              from inventory_view.oc_transactionoptimized) t2 on 
                (trim(t1.omni_stid) = trim(t2.StockAreaId) and
                trim(t1.item_id) = trim(t2.ItemId))
        where t2.StockAreaId is not null) t
  group by CustomerId,cpckey
"""
customers_df = sqlContext.sql(sql)
customers_df.createOrReplaceTempView("vw_customers")

# COMMAND ----------

display(customers_df)

# COMMAND ----------

# DBTITLE 1,Get item configuration
sql = """
  select distinct t2.global_item_id,  t3.*
  from (select distinct cpckey from vw_customers where n_obs > 0) t1 
  left join (select distinct global_item_id,cpckey,item_id,omni_stid
             from vw_items_loc
             where omni_stid is not null) t2 on (trim(t1.cpckey) = trim(t2.cpckey))
  left join (select distinct CustomerId, FacilityId, StockAreaId, ItemId, ItemName,
                             case
                                when lower(StrengthUnits) = 'mcg' then 0.001*cast(IssuePackageDose as float)
                                when lower(StrengthUnits) = 'g' then 1000*cast(IssuePackageDose as float)
                                else cast(IssuePackageDose as float)
                             end as IssuePackageDose_norm,
                             if(lower(StrengthUnits) in ('mcg','g'),'mg',lower(StrengthUnits)) as StrengthUnits_norm
            from omniproductionoc.transactionappend 
            where TransactionDttm >= '2020-01-01' and
                  TransactionType in ('I','R') and
                  IsPatientSpecificBin = 'false' and 
                  IsPatientOwnMed = 'false') t3 on 
                  (trim(t2.item_id) = trim(t3.ItemId) and 
                  trim(t2.omni_stid) = trim(t3.StockAreaId))
  """
items_cfg_df = sqlContext.sql(sql)
items_cfg_df.createOrReplaceTempView("vw_items_config")

# COMMAND ----------

# DBTITLE 1,Data quality check
# MAGIC %sql 
# MAGIC select global_item_id,CustomerId , count(1) as n, 
# MAGIC        count(distinct IssuePackageDose_norm, StrengthUnits_norm) as n_configs, 
# MAGIC        count(distinct StrengthUnits_norm) as n_units
# MAGIC from vw_items_config
# MAGIC where lower(ItemName) like '%propofol%' and CustomerId is not null
# MAGIC group by global_item_id,CustomerId

# COMMAND ----------

# DBTITLE 1,Get the transactions for all relevant selected items from all customers
# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists ds_development.rk_covid_propofol_trx;
# MAGIC create table ds_development.rk_covid_propofol_trx as
# MAGIC select t2.global_item_id, t3.*
# MAGIC from (select distinct cpckey from ds_development.rk_covid_propofol_customers where n_obs > 0) t1
# MAGIC left join (select distinct global_item_id,cpckey,item_id,omni_stid
# MAGIC            from ds_development.rk_covid_propofol_itemloc 
# MAGIC            where omni_stid is not null) t2 on (trim(t1.cpckey) = trim(t2.cpckey))
# MAGIC left join (select CustomerId,FacilityId,StockAreaId,ItemId, 
# MAGIC                   TransactionDttm,
# MAGIC                   TransactionType,DeltaQuantity
# MAGIC            from inventory_view.oc_transactionoptimized
# MAGIC            where ((TransactionDttm >= '2019-01-01' and TransactionDttm < '2019-04-16') or (TransactionDttm >= '2020-01-01')) and
# MAGIC                  TransactionType in ('I','R') and 
# MAGIC                  BeginBinInventory > 0 and BeginBinInventory <> EndBinInventory) t3 on (trim(t2.item_id) = trim(t3.ItemID) and 
# MAGIC                                                                                         trim(t2.omni_stid) = trim(t3.StockAreaId));
# MAGIC                                                                                         
# MAGIC select * from ds_development.rk_covid_propofol_trx limit 10;                                                                                        

# COMMAND ----------

# DBTITLE 1,Create the time series
# MAGIC %sql
# MAGIC 
# MAGIC drop table if exists ds_development.rk_covid_propofol_time_series;
# MAGIC create table ds_development.rk_covid_propofol_time_series as
# MAGIC select t1.CustomerId,t1.FacilityId,t1.StockAreaId,t1.global_item_id,
# MAGIC        date_format(TransactionDttm, 'yyyy-MM-dd') as date_,
# MAGIC        max(date_format(TransactionDttm, 'yyyy')) as yr_,
# MAGIC        max(date_format(TransactionDttm, 'MM-dd')) as md_,
# MAGIC        sum(t2.DeltaQuantity) as qty_issued, 
# MAGIC        sum(t2.DeltaQuantity*t1.IssuePackageDose_norm) as amt_issued,
# MAGIC        t1.StrengthUnits_norm as amt_issued_units
# MAGIC from ds_development.rk_covid_propofol_cfg t1
# MAGIC left join ds_development.rk_covid_propofol_trx t2 on (t1.CustomerId = t2.CustomerId and
# MAGIC                                                       t1.FacilityId = t2.FacilityId and
# MAGIC                                                       t1.StockAreaId = t2.StockAreaId and
# MAGIC                                                       t1.ItemId = t2.ItemId)
# MAGIC where lower(t1.ItemName) like '%propofol%'
# MAGIC group by t1.global_item_id,t1.StrengthUnits_norm,t1.CustomerId,t1.FacilityId,t1.StockAreaId,date_format(TransactionDttm, 'yyyy-MM-dd') 
# MAGIC order by t1.global_item_id,t1.StrengthUnits_norm,t1.CustomerId,t1.FacilityId,t1.StockAreaId,date_format(TransactionDttm, 'yyyy-MM-dd') 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select customer_id, count(distinct item_id), count(distinct item_id, omni_stid)
# MAGIC from inventory_view.oc_transactions_db
# MAGIC where lower(item_name) like 'propofol%' and 
# MAGIC       xact_dati >= '2020-01-01' and
# MAGIC       xfer_type in ('I','R') 
# MAGIC group by customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select CustomerId, count(distinct ItemId), count(distinct ItemId, StockAreaId) 
# MAGIC from ds_development.rk_covid_propofol_trx 
# MAGIC where TransactionDttm >= '2020-01-01'
# MAGIC group by CustomerId
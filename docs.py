'''Script for building appts daily rollup'''
import polars as pl
from datetime import datetime
import sqlglot
import sqlglot.expressions as exp
import connectorx as cx
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql as types

queries = {
    'legacy_appts': 'select posting_date as date, facility_id, provider_id, firm_id,  appt_count as legacy_appt from legacy.legacyappointments',
    'appointments_created': f'select row_number() over() as index, a.appointment_id, a.facility_id, a.firm_id,  a.provider_id as primary_provider_id, bil.primary_biller_id as provider_id, a.appointment_date_created_ld, a.appointment_end_date_ld, a.appointment_status, a.appointment_type, check_in_time_ld, check_out_time_ld from "modmed".appointment a LEFT JOIN modmed.bill  bil ON a.appointment_id = bil.appointment_id where a.appointment_date_created_ld >= \'2021-01-01\' and a.appointment_date_created_ld <= \'{datetime.now().strftime("%Y-%m-%d")}\'',
    'appointments': f'select row_number() over() as index, a.appointment_id, a.facility_id, a.firm_id, a.provider_id as primary_provider_id, bil.primary_biller_id as provider_id, a.appointment_start_date_ld, a.appointment_end_date_ld, a.appointment_status, a.appointment_type, check_in_time_ld, check_out_time_ld, a.rescheduled_date_ld as rescheduled_date, a.appointment_date_cancelled_ld as cancelled_date from "modmed".appointment a LEFT JOIN modmed.bill  bil ON a.appointment_id = bil.appointment_id where a.appointment_start_date_ld >= \'2021-01-01\' and a.appointment_start_date_ld < \'{datetime.now().strftime("%Y-%m-%d")}\'',
    'provider_biller': 'select "Primary Provider ID", "Primary Biller ID", "Reporting Provider ID" from "mapping".modmed_provider_mapping',
    'financial_category' : 'select financial_category_id, firm_id, category_name, is_self_pay, category_type, patient_responsible, rcm_category, active, firm_global_id FROM modmed.financial_category',
    'financial_category_legacy':'select financial_category_id, firm_id, category_name, is_self_pay, category_type, patient_responsible, rcm_category, active, firm_global_id FROM modmed_legacy.financial_category'
}

def find_index(columns):
    index_column = False
    for column in columns:
        if 'balance' in column:
            index_column = column
            break
        elif 'amount' in column:
            index_column = column
            break
        elif 'count' in column:
            index_column = column
            break
        elif column == 'index':
            index_column = column
            break
    return index_column

def parse_columns(sql_query):
    column_names = []
    for expression in sqlglot.parse_one(sql_query).find(exp.Select).args["expressions"]:
        if isinstance(expression, exp.Alias):
            column_names.append(expression.text("alias"))
        elif isinstance(expression, exp.Column):
            column_names.append(expression.text("this"))
    return column_names

def build_data(sql_query):
    columns = parse_columns(sql_query)
    index_column = find_index(columns)
    connect_string = '''postgresql://postgres:gT52AdJcN2V1xaUjcEM5@10.16.2.5:5432/datawarehouse'''
    if index_column:
        data = cx.read_sql(conn=connect_string, query=sql_query, return_type='polars', partition_on=index_column, partition_num=4)
    else:
        data = cx.read_sql(conn=connect_string, query=sql_query, return_type='polars')
    return data

def appointment_summary():
    active_appts_status_exclude = ['NO_SHOW', 'CANCELLED']
    completed_appts_status_exclude = ['NO_SHOW', 'CANCELLED', 'PENDING']
    billable_exclusions = ['Suture Removal', 'Wound Check', 'Wound Laser','Post op', 'Nurse Visit', 'One Week Post-Op', 'Next Day Post Op', 'Study Patient']
    provider_biller = build_data(queries['provider_biller'])
    data_appointments = build_data(queries['appointments'])
    data_financial = build_data(queries['financial_category'])
    data_appointments = data_appointments.unique(subset=['appointment_id'])
    data_appointments = data_appointments.with_columns(date=pl.col('appointment_start_date_ld').cast(pl.Date()))
    data_appointments = data_appointments.with_columns(cancelled_date=pl.col('cancelled_date').cast(pl.Date()))
    data_appointments = data_appointments.with_columns(rescheduled_date=pl.col('rescheduled_date').cast(pl.Date()))

    data_appointments = data_appointments.with_columns(provider_id=pl.when(pl.col('provider_id').is_null()).then(pl.col('primary_provider_id')).otherwise(pl.col('provider_id')))
    data_appointments = data_appointments.join(provider_biller, left_on=['provider_id', 'primary_provider_id'], right_on=['Primary Biller ID', 'Primary Provider ID'], how='left')
    data_appointments = data_appointments.with_columns(provider_id=pl.when(pl.col('Reporting Provider ID').is_not_null()).then(pl.col('Reporting Provider ID')).otherwise(pl.col('provider_id')))

    data_appointments = data_appointments.join(data_financial, left_on=['firm_id'], right_on=['firm_id'], how='left')

    data_appointments = data_appointments.with_columns(total_cancelled=pl.when(pl.col('appointment_status') == 'CANCELLED').then(pl.lit(1.0)).otherwise(0.0))
    data_appointments = data_appointments.with_columns(same_day_cancelled=pl.when((pl.col('appointment_status') == 'CANCELLED') & (pl.col('date').eq(pl.col('cancelled_date')))).then(pl.lit(1.0)).otherwise(0.0))
    data_appointments = data_appointments.with_columns(late_reschedule=pl.when(pl.col('date').eq(pl.col('rescheduled_date'))).then(pl.lit(1.0)).otherwise(0.0))
    data_appointments = data_appointments.with_columns(no_show=pl.when(pl.col('appointment_status') == 'NO_SHOW').then(pl.lit(1.0)).otherwise(0.0))
    data_appointments = data_appointments.with_columns(pending=pl.when(pl.col('appointment_status') == 'PENDING').then(pl.lit(1.0)).otherwise(0.0))
    data_appointments = data_appointments.with_columns(completed=pl.when(~pl.col('appointment_status').is_in(completed_appts_status_exclude)).then(pl.lit(1.0)).otherwise(0.0))
    data_appointments = data_appointments.with_columns(active=pl.when(~pl.col('appointment_status').is_in(active_appts_status_exclude)).then(pl.lit(1.0)).otherwise(0.0))
    data_appointments = data_appointments.with_columns(billable=pl.when(~pl.col('appointment_status').is_in(completed_appts_status_exclude) & (~pl.col('appointment_type').is_in(billable_exclusions))).then(pl.lit(1.0)).otherwise(0.0))
    
    

    appointments = data_appointments.group_by(['date', 'facility_id', 'provider_id','firm_id','category_name']).agg([
        pl.col('appointment_id').count().alias('scheduled'),
        pl.col('total_cancelled').sum().alias('total_cancelled'),
        pl.col('same_day_cancelled').sum().alias('same_day_cancelled'),
        pl.col('late_reschedule').sum().alias('late_reschedule'),
        pl.col('no_show').sum().alias('no_show'),
        pl.col('pending').sum().alias('pending'),
        pl.col('completed').sum().alias('completed'),
        pl.col('active').sum().alias('active'),
        pl.col('billable').sum().alias('billable'),
        ])

    return appointments

def legacy_summary(appointments):
    legacy_appts = build_data(queries['legacy_appts'])
    financial_legacy = build_data(queries['financial_category_legacy'])
    legacy_appts = legacy_appts.with_columns(date=pl.col("date").str.strptime(pl.Date, "%m/%d/%Y", strict=True))
    legacy_appts = legacy_appts.group_by(['date', 'facility_id', 'provider_id','firm_id']).agg(pl.col('legacy_appt').sum())
    legacy_appts = legacy_appts.join(financial_legacy, left_on=['firm_id'], right_on=['firm_id'], how='left')
    appointments = appointments.join(legacy_appts, on=['date', 'facility_id', 'provider_id', 'firm_id','category_name'], how='outer')
    appointments = appointments.fill_null(0)
    appointments = appointments.with_columns(scheduled=pl.col('scheduled').add(pl.col('legacy_appt')))
    appointments = appointments.with_columns(completed=pl.col('completed').add(pl.col('legacy_appt')))
    appointments = appointments.with_columns(billable=pl.col('billable').add(pl.col('legacy_appt')))
    appointments = appointments.drop(['legacy_appt'])
    return appointments

def ach_summary():
    completed_appts_status_exclude = ['NO_SHOW', 'CANCELLED', 'PENDING']
    data_appointments = build_data(queries['appointments'])
    fi_category = build_data(queries['financial_category'])
    data_appointments = data_appointments.unique(subset=['appointment_id'])
    data_appointments = data_appointments.with_columns(date=pl.col('appointment_start_date_ld').cast(pl.Date()))
    data_appointments = data_appointments.with_columns(provider_id=pl.when(pl.col('primary_provider_id').is_null()).then(pl.col('provider_id')).otherwise(pl.col('primary_provider_id')))
    data_appointments = data_appointments.filter(~pl.col('appointment_status').is_in(completed_appts_status_exclude))
    data_appointments = data_appointments.to_pandas()
    data_appointments = data_appointments.set_index('appointment_id')
    blocks = {}
    for details, appts in data_appointments.groupby(['date', 'facility_id', 'provider_id']):
        if not blocks.get(details[0], False):
            blocks[details[0]] = {}
        if not blocks[details[0]].get(details[1], False):
            blocks[details[0]][details[1]] = {}
        if not blocks[details[0]][details[1]].get(details[2], False):
            blocks[details[0]][details[1]][details[2]] = []
        block_start = False
        block_end = False
        for appointment_id, appt_info in appts.sort_values(by=['appointment_start_date_ld']).iterrows():
            if not block_start:
                block_start = appt_info['appointment_start_date_ld']
                block_end = appt_info['appointment_end_date_ld']
            else:
                if appt_info['appointment_start_date_ld'] < block_end:
                    if appt_info['appointment_end_date_ld'] > block_end:
                        block_end = appt_info['appointment_end_date_ld']
                else:
                    blocks[details[0]][details[1]][details[2]].append((block_start, block_end))
                    block_start = appt_info['appointment_start_date_ld']
                    block_end = appt_info['appointment_end_date_ld']
        blocks[details[0]][details[1]][details[2]].append((block_start, block_end))
    ach_data = []
    for date, date_appts in blocks.items():
        for facility, facility_appts in date_appts.items():
            for provider, provider_appts in facility_appts.items():
                ach_total = 0
                for (start_date, end_date) in provider_appts:
                    ach_total += ((end_date - start_date).total_seconds() / 60.0)
                ach_data.append([date.date(), facility, provider, round(ach_total/60, 2)])
    ach_data = pl.DataFrame(ach_data, schema={'date':pl.Date, 'facility_id':pl.Utf8, 'provider_id':pl.Utf8, 'clinic_hours':pl.Float32})
    ach_data = data_appointments.join(ach_data,left_on=['provider_id'],right_on=['provider_id'],how='inner')
    ach_data = ach_data[['date','facility_id','provider_id','firm_id','clinic_hours']]
    ach_data = ach_data.join(fi_category,left_on=['firm_id'],right_on=['firm_id'],how='left')
    ach_data = ach_data[['date','facility_id','provider_id','firm_id','category_name','clinic_hours']]

    return ach_data

def created_appts():
    provider_biller = build_data(queries['provider_biller'])
    appts_created = build_data(queries['appointments_created'])
    fin_category = build_data(queries['financial_category'])
    appts_created = appts_created.unique(subset=['appointment_id'])
    appts_created = appts_created.with_columns(date=pl.col('appointment_date_created_ld').cast(pl.Date()))

    appts_created = appts_created.with_columns(provider_id=pl.when(pl.col('provider_id').is_null()).then(pl.col('primary_provider_id')).otherwise(pl.col('provider_id')))
    appts_created = appts_created.join(provider_biller, left_on=['provider_id', 'primary_provider_id'], right_on=['Primary Biller ID', 'Primary Provider ID'], how='left')
    appts_created = appts_created.with_columns(provider_id=pl.when(pl.col('Reporting Provider ID').is_not_null()).then(pl.col('Reporting Provider ID')).otherwise(pl.col('provider_id')))
    appts_created = appts_created.join(fin_category,left_on=['firm_id'], right=['firm_id'], how='left')
    appts_created = appts_created.group_by(['date', 'facility_id', 'provider_id','firm_id','category_name']).agg(pl.col('appointment_id').count().alias('created'))
    return appts_created

def appts_rollup():
    appointments = appointment_summary()
    appointments = legacy_summary(appointments)
    ach_data = ach_summary()
    appts_created = created_appts()
    rollup = appointments.join(ach_data, on=['date', 'facility_id', 'provider_id','firm_id','category_name'], how='outer')
    rollup = rollup.join(appts_created, on=['date', 'facility_id', 'provider_id','firm_id','category_name'], how='outer')
    rollup = rollup.fill_null(0)
    display(rollup)
    #connect_string = '''postgresql://10.16.2.5:5432/datawarehouse'''
    #engine = create_engine(connect_string)
    #rollup.to_sql('modmed_appts_rollup_dev', engine, schema='custom_dev', if_exists='replace', index=False, dtype={'date':types.TIMESTAMP(0)})

if __name__ == "__main__":
    appts_rollup()

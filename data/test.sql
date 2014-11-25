-- The process will only start if there are new leaves. This table is created by another process.
drop table ${db_name}.ind_new_leaf_recs;

insert into ${db_name}.leaf_conv_lkp_dnd
	select
		0 as batch_id
		,event_name
		,concept_name
		,${run_dt} as insert_date
		,event_date
	from
		${db_name}.leaf_conv_proc_list
	group by
		1,2,3,4,5;

insert into ${db_name}.leaf_conv_in_dnd
	select
		b.batch_id
		,trim(a.attribute_type) as attribute_type
		,trim(a.attribute_value) as attribute_value
		,'prob' as attribute_nature
		,'v2' as process_using
		,0 as ind_processed
	from
		${db_name}.leaf_conv_proc_list a
		left join
		${db_name}.leaf_conv_lkp_dnd b
		on a.event_name = b.event_name
			and a.concept_name = b.concept_name
			and a.event_date = b.campaign_date;


-- As long as this table exists, the process will not start
-- IF YOU ARE RUNNING THIS MANUALLY, MAKE SURE THAT THE TABLE GETS POPULATED
create table ${db_name}.${table_prefix}ref_event as
(
	select
		batch_id
		,cast(attribute_value as decimal(18,0)) as leaf_categ_id
	from
		${db_name}.leaf_conv_in_dnd
	where
		ind_processed = 0
			and attribute_type = 'leaf'
)
with data primary index(leaf_categ_id);

update ${db_name}.leaf_conv_in_dnd set ind_processed = 2 where attribute_type = 'leaf' and ind_processed = 0;

-- IF YOU ARE RUNNING THIS MANUALLY, MAKE SURE THAT THE TABLE GETS POPULATED
create table ${db_name}.${table_prefix}ref_leaf as
(
	select
		*
	from
		${db_name}.plm_bayes_finlist_dnd
	where
		target_leaf in (select leaf_categ_id from ${db_name}.${table_prefix}ref_event group by 1)
)
with data primary index(target_leaf);
collect stats on ${db_name}.${table_prefix}ref_leaf column(browse_leaf);
collect stats on ${db_name}.${table_prefix}ref_leaf column(bid_leaf);
collect stats on ${db_name}.${table_prefix}ref_leaf column(bin_leaf);
collect stats on ${db_name}.${table_prefix}ref_leaf column(watch_leaf);
collect stats on ${db_name}.${table_prefix}ref_leaf column(target_leaf);


--PROCESS BROWSERS
create multiset table ${db_name}.${table_prefix}plm_browser_tmp as
(
	select
		b.buyer_id as user_id
		,b.item_id
		,b.leaf_categ_id
	from
		bbdnb_sd b
	where
		b.sessn_start_dt between ${run_dt} - 10 and ${run_dt} - 1
			and b.browse_site_id in (${site_id})
			and b.leaf_categ_id in (select browse_leaf from ${db_name}.${table_prefix}ref_leaf where browse_leaf is not null group by 1)
			and b.buyer_id in (select user_id from ${db_name}.te_experiment_users_dnd)

)
with data primary index(user_id,item_id);

create multiset table ${db_name}.${table_prefix}plm_browser as
(
	select
		user_id
		,leaf_categ_id
		,count(*) as ctt
	from
		${db_name}.${table_prefix}plm_browser_tmp
	group by
		1,2
)
with data primary index(user_id,leaf_categ_id);
collect stats on ${db_name}.${table_prefix}plm_browser column(user_id);
collect stats on ${db_name}.${table_prefix}plm_browser column(leaf_categ_id);

create multiset table ${db_name}.${table_prefix}plm_usr_browse_ct as
(
	select
		a.user_id
		,b.target_leaf
		,b.order_num
		,cast(a.ctt as decimal (18,0)) as ctt
	from
		(select * from ${db_name}.${table_prefix}plm_browser where user_id in (select user_id from (select user_id, count(*) as ctt from ${db_name}.${table_prefix}plm_browser group by 1 having count(*) = 1) aa))  a
		left join
		${db_name}.${table_prefix}ref_leaf b
		on a.leaf_categ_id = b.browse_leaf
)
with data primary index(user_id,target_leaf);

insert into ${db_name}.${table_prefix}plm_usr_browse_ct
	select
		a.user_id
		,b.target_leaf
		,b.order_num
		,cast(a.ctt as decimal (18,0)) as ctt
	from
		(select * from ${db_name}.${table_prefix}plm_browser where user_id in (select user_id from (select user_id, count(*) as ctt from ${db_name}.${table_prefix}plm_browser group by 1 having count(*) = 2) aa))  a
		left join
		${db_name}.${table_prefix}ref_leaf b
		on a.leaf_categ_id = b.browse_leaf;

insert into ${db_name}.${table_prefix}plm_usr_browse_ct
	select
		a.user_id
		,b.target_leaf
		,b.order_num
		,cast(a.ctt as decimal (18,0)) as ctt
	from
		(select * from ${db_name}.${table_prefix}plm_browser where user_id in (select user_id from (select user_id, count(*) as ctt from ${db_name}.${table_prefix}plm_browser group by 1 having count(*) = 3) aa))  a
		left join
		${db_name}.${table_prefix}ref_leaf b
		on a.leaf_categ_id = b.browse_leaf;

insert into ${db_name}.${table_prefix}plm_usr_browse_ct
	select
		a.user_id
		,b.target_leaf
		,b.order_num
		,cast(a.ctt as decimal (18,0)) as ctt
	from
		(select * from ${db_name}.${table_prefix}plm_browser where user_id in (select user_id from (select user_id, count(*) as ctt from ${db_name}.${table_prefix}plm_browser group by 1 having count(*) between 4 and 10) aa))  a
		left join
		${db_name}.${table_prefix}ref_leaf b
		on a.leaf_categ_id = b.browse_leaf;

insert into ${db_name}.${table_prefix}plm_usr_browse_ct
	select
		a.user_id
		,b.target_leaf
		,b.order_num
		,cast(a.ctt as decimal (18,0)) as ctt
	from
		(select * from ${db_name}.${table_prefix}plm_browser where user_id in (select user_id from (select user_id, count(*) as ctt from ${db_name}.${table_prefix}plm_browser group by 1 having count(*) > 10) aa))  a
		left join
		${db_name}.${table_prefix}ref_leaf b
		on a.leaf_categ_id = b.browse_leaf;


--PROCESS BIDDERS
create multiset table ${db_name}.${table_prefix}plm_bidder_tmp as
(
	select
		bdr_id as user_id
		,item_id
		,cast(null as decimal(9,0)) as bid_leaf
	from
		dw_bids_ended a
	where
		a.bid_dt between ${run_dt} - 90 and ${run_dt} - 1
			and a.bdr_site_id in (${site_id})
			and a.auct_end_dt >= ${run_dt} - 91
			and a.bdr_id in (select user_id from ${db_name}.te_experiment_users_dnd)
)
with data primary index(item_id,user_id);

update
	a
from
	${db_name}.${table_prefix}plm_bidder_tmp a
	,(
		select
			item_id
			,leaf_categ_id
		from
			dw_lstg_item
		where
			leaf_categ_id in (select bid_leaf from ${db_name}.${table_prefix}ref_leaf where bid_leaf is not null group by 1)
			and item_id in (select item_id from ${db_name}.${table_prefix}plm_bidder_tmp group by 1)
				and item_site_id in (${site_id})
				and auct_end_dt >= ${run_dt} - 91
	) b
set
	bid_leaf = b.leaf_categ_id
where
	a.item_id = b.item_id;

create multiset table ${db_name}.${table_prefix}plm_bidder as
(
	select
		user_id
		,bid_leaf
		,count(*) as ctt
	from
		${db_name}.${table_prefix}plm_bidder_tmp
	where
		bid_leaf is not null
	group by
		1,2
)
with data primary index(user_id, bid_leaf);
collect stats on ${db_name}.${table_prefix}plm_bidder column(user_id);
collect stats on ${db_name}.${table_prefix}plm_bidder column(bid_leaf);

create multiset table ${db_name}.${table_prefix}plm_usr_bid_ct as
(
	select
		a.user_id
		,b.target_leaf
		,b.order_num
		,a.ctt
	from
		${db_name}.${table_prefix}plm_bidder a
		left join
		${db_name}.${table_prefix}ref_leaf b
		on a.bid_leaf = b.bid_leaf
)
with data primary index(user_id,target_leaf);


-- BUILD BASE TABLE
create multiset table ${db_name}.${table_prefix}plm_base as
(
	select
		user_id
		,target_leaf
	from
		(
			select user_id, target_leaf from ${db_name}.${table_prefix}plm_usr_browse_ct group by 1,2
			union
			select user_id, target_leaf from ${db_name}.${table_prefix}plm_usr_bid_ct group by 1,2
		) a
	where
		user_id in (select user_id from ${db_name}.te_experiment_users_dnd)
	group by
		1,2
)
with data primary index(user_id, target_leaf);


create multiset  table ${db_name}.${table_prefix}plm_combined_signals as
(
	select
		a.user_id
		,a.target_leaf
<repeat order=1>		,coalesce(max(case when b.order_num = ${order_num} then b.ctt else 0 end),0) as browse_leaf_c${var_name}</repeat>

<repeat order=2>		,coalesce(max(case when b.order_num = ${order_num2} then b.ctt else 0 end),0) as bid_leaf_${var2_name}</repeat>
	from
		${db_name}.${table_prefix}plm_base a
		left join
		${db_name}.${table_prefix}plm_usr_browse_ct b
		on a.user_id = b.user_id
			and a.target_leaf = b.target_leaf
		left join
		${db_name}.${table_prefix}plm_usr_bid_ct c
		on a.user_id = c.user_id
			and a.target_leaf = c.target_leaf
	group by
		1,2
)
with data primary index (user_id,target_leaf);


create multiset table ${db_name}.${table_prefix}plm_scored_usr_dnd as
(
	select
		user_id
		,target_leaf
		,(case when browse_leaf_ct01 > 0 then 1 else 0 end + case when browse_leaf_ct02 > 0 then 1 else 0 end + case when browse_leaf_ct03 > 0 then 1 else 0 end  + case when browse_leaf_ct04 > 0 then 1 else 0 end +
		case when browse_leaf_ct05 > 0 then 1 else 0 end + case when browse_leaf_ct06 > 0 then 1 else 0 end + case when browse_leaf_ct07 > 0 then 1 else 0 end + case when browse_leaf_ct08 > 0 then 1 else 0 end + 
		case when browse_leaf_ct09 > 0 then 1 else 0 end + case when browse_leaf_ct10 > 0 then 1 else 0 end + case when browse_leaf_ct11 > 0 then 1 else 0 end + case when browse_leaf_ct12 > 0 then 1 else 0 end + 
		case when browse_leaf_ct13 > 0 then 1 else 0 end + case when browse_leaf_ct14 > 0 then 1 else 0 end + case when browse_leaf_ct15 > 0 then 1 else 0 end + case when browse_leaf_ct16 > 0 then 1 else 0 end + 
		case when browse_leaf_ct17 > 0 then 1 else 0 end + case when browse_leaf_ct18 > 0 then 1 else 0 end + case when browse_leaf_ct19 > 0 then 1 else 0 end + case when browse_leaf_ct20 > 0 then 1 else 0 end)
		as rel_browse_cnt

		,(case when bid_leaf_ct01 > 0 then 1 else 0 end + case when bid_leaf_ct02 > 0 then 1 else 0 end + case when bid_leaf_ct03 > 0 then 1 else 0 end  + case when bid_leaf_ct04 > 0 then 1 else 0 end +
		case when bid_leaf_ct05 > 0 then 1 else 0 end + case when bid_leaf_ct06 > 0 then 1 else 0 end + case when bid_leaf_ct07 > 0 then 1 else 0 end + case when bid_leaf_ct08 > 0 then 1 else 0 end + 
		case when bid_leaf_ct09 > 0 then 1 else 0 end + case when bid_leaf_ct10 > 0 then 1 else 0 end + case when bid_leaf_ct11 > 0 then 1 else 0 end + case when bid_leaf_ct12 > 0 then 1 else 0 end + 
		case when bid_leaf_ct13 > 0 then 1 else 0 end + case when bid_leaf_ct14 > 0 then 1 else 0 end + case when bid_leaf_ct15 > 0 then 1 else 0 end + case when bid_leaf_ct16 > 0 then 1 else 0 end + 
		case when bid_leaf_ct17 > 0 then 1 else 0 end + case when bid_leaf_ct18 > 0 then 1 else 0 end + case when bid_leaf_ct19 > 0 then 1 else 0 end + case when bid_leaf_ct20 > 0 then 1 else 0 end)
		as rel_bid_cnt

		,(case when browse_leaf_ct01 > 0 or  browse_leaf_ct02 > 0 or  browse_leaf_ct03 > 0 or  browse_leaf_ct04 > 0 or  browse_leaf_ct05 > 0 then 1 else 0 end) as rel_browse_5
		,(case when bid_leaf_ct01 > 0 or  bid_leaf_ct02 > 0 or  bid_leaf_ct03 > 0 or  bid_leaf_ct04 > 0 or  bid_leaf_ct05 > 0 then 1 else 0 end) as rel_bid_5

		,cast(-4.672413907 + (5.272812141 * rel_browse_cnt) + (-1.578197335 * rel_bid_cnt) + (4.756467787 * rel_browse_5) + (5.007209337 * rel_bid_5)) as decimal(18,5)) as int_calc
		,cast(1 as decimal (18,5))/cast((1 + exp(-1 * int_calc)) as decimal(18,5)) as  prob_score
	from
		${db_name}.${table_prefix}plm_combined_signals
)with data primary index(user_id,target_leaf);

-- Delete buyers in the last 2 weeks
delete from ${db_name}.${table_prefix}plm_scored_usr_dnd
where (user_id, target_leaf) in
	(
		select
			buyer_id as user_id
			,leaf_categ_id as target_leaf
		from
			dw_checkout_trans
		where
			leaf_categ_id in (select attribute_value from ${db_name}.leaf_conv_proc_list where attribute_type = 'leaf' group by 1)
				and created_dt between ${run_dt} - 28 and ${run_dt} - 1
				and site_id in (${site_id})
		group by
			1,2
	);


insert into ${db_name}.leaf_conv_out_dnd
	select
		b.batch_id
		,a.user_id
		,max(a.prob_score) as prob_score
	from
		${db_name}.${table_prefix}plm_scored_usr_dnd a
		inner join
		${db_name}.${table_prefix}ref_event b
		on a.target_leaf = b.leaf_categ_id
	where
		a.user_id mod 5 = 0
	group by
		1,2;

insert into ${db_name}.leaf_conv_out_dnd
	select
		b.batch_id
		,a.user_id
		,max(a.prob_score) as prob_score
	from
		${db_name}.${table_prefix}plm_scored_usr_dnd a
		inner join
		${db_name}.${table_prefix}ref_event b
		on a.target_leaf = b.leaf_categ_id
	where
		a.user_id mod 5 = 1
	group by
		1,2;

insert into ${db_name}.leaf_conv_out_dnd
	select
		b.batch_id
		,a.user_id
		,max(a.prob_score) as prob_score
	from
		${db_name}.${table_prefix}plm_scored_usr_dnd a
		inner join
		${db_name}.${table_prefix}ref_event b
		on a.target_leaf = b.leaf_categ_id
	where
		a.user_id mod 5 = 2
	group by
		1,2;

insert into ${db_name}.leaf_conv_out_dnd
	select
		b.batch_id
		,a.user_id
		,max(a.prob_score) as prob_score
	from
		${db_name}.${table_prefix}plm_scored_usr_dnd a
		inner join
		${db_name}.${table_prefix}ref_event b
		on a.target_leaf = b.leaf_categ_id
	where
		a.user_id mod 5 = 3
	group by
		1,2;

insert into ${db_name}.leaf_conv_out_dnd
	select
		b.batch_id
		,a.user_id
		,max(a.prob_score) as prob_score
	from
		${db_name}.${table_prefix}plm_scored_usr_dnd a
		inner join
		${db_name}.${table_prefix}ref_event b
		on a.target_leaf = b.leaf_categ_id
	where
		a.user_id mod 5 = 4
	group by
		1,2;


update
	a
from
	${db_name}.leaf_conv_in_dnd a
	,(select batch_id from ${db_name}.${table_prefix}ref_event group by 1) b
 set
 	ind_processed = 1
 where
 	a.batch_id = b.batch_id;


drop table ${db_name}.${table_prefix}plm_browser_tmp;
drop table ${db_name}.${table_prefix}plm_browser;
drop table ${db_name}.${table_prefix}plm_bidder_tmp;
drop table ${db_name}.${table_prefix}plm_bidder;
drop table ${db_name}.${table_prefix}plm_buyer;
drop table ${db_name}.${table_prefix}plm_watcher_tmp;
drop table ${db_name}.${table_prefix}plm_watcher;
drop table ${db_name}.${table_prefix}plm_base;
drop table ${db_name}.${table_prefix}plm_usr_browse_ct;
drop table ${db_name}.${table_prefix}plm_usr_bid_ct;
drop table ${db_name}.${table_prefix}plm_usr_bin_ct;
drop table ${db_name}.${table_prefix}plm_usr_watch_ct;
drop table ${db_name}.${table_prefix}plm_combined_signals;
drop table ${db_name}.${table_prefix}plm_scored_usr_dnd;
drop table ${db_name}.leaf_conv_proc_list;
drop table ${db_name}.${table_prefix}ref_leaf;
drop table ${db_name}.${table_prefix}ref_event;

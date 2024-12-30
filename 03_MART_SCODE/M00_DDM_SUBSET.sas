%macro gen_DDM_CINID_Level(key=, tarlib=);
%if %sysfunc(exist(&tarlib..DDM_&key._AFTER_&st_BackMth.)) %then %do;
	%put ---- NCB LOG ---- : !! &tarlib..DDM_&key._AFTER_&st_BackMth. is already existed.;
%end;
%else %do;
	/* ************************************************************************************************* */
	/* Start of Step 1 - generate key and filtering by specific date range */
	data v_temp/view=v_temp;
		set fact.ddm_tx_master_allhist_&st_RptMth.;
		if missing(sasflag_exceptional);
		if &dt_BBegMth. <= d_default_start_date <= &dt_BEndMth.;
		/*if d_ac_date <= intnx("MONTH",&dt_RptMth.,1,"END"); */
		/*for example: d_ac_date=20160401 d_RptMth=20160331 */
		if not missing(key_&key.); 
	run;
	proc sort data=v_temp out=work.ddm_subset_detail; 
		by key_&key. n_default_type d_default_start_date descending d_ac_date; 
	run;
	data work.ddm_subset_detail_v1;
		set work.ddm_subset_detail;
		by key_&key. n_default_type d_default_start_date; 
		if first.n_default_type then output;
	run;
	/* Finished of Step 1 - generate key and filtering by specific date range */
	/* ************************************************************************************************* */
	/* Start of Step 2 - get the latest available demographic information */
	proc sort data=work.ddm_subset_detail out=work.key_ddm_latest(
			keep=key_&key. c_ind_mainland n_svid n_bk n_cin c_cust_id c_customer_type n_ind_non_retail d_ac_date 
			); 
		by key_&key. descending d_ac_date; 
	run;
	data work.key_ddm_latest_v1;
		set work.key_ddm_latest ;
		by key_&key.; 
		if first.key_&key. then output;
	run;
	/* end of Step 2 */
	/* ************************************************************************************************* */
	/* ************************************************************************************************* */
	/* start of Step 3 - prepare all the first available default date under the specific data window */
	data work.ddm_subset_detail_v2;
		retain c_default_type_d1		- c_default_type_d4 		"  ";
		retain d_default_start_date_d1	- d_default_start_date_d4 	.;
		retain d_default_end_date_d1	- d_default_end_date_d4 	.;
		retain d_ac_date_d1				- d_ac_date_d4 				.;
		retain n_ind_non_retail_d1		- n_ind_non_retail_d4 		.;

		array t_dtype 	{4} c_default_type_d1		- c_default_type_d4;
		array t_dstart 	{4} d_default_start_date_d1	- d_default_start_date_d4;
		array t_dend 	{4} d_default_end_date_d1	- d_default_end_date_d4;
		array t_acdate	{4} d_ac_date_d1			- d_ac_date_d4;
		array t_nretail	{4} n_ind_non_retail_d1		- n_ind_non_retail_d4;

		set work.ddm_subset_detail_v1;
		by key_&key. n_default_type; 

		if first.key_&key. then do;
			do i = 1 to 4;
				call missing(t_acdate(i));
				call missing(t_dtype(i));
				call missing(t_dstart(i));
				call missing(t_dend(i));
				call missing(t_nretail(i));
			end;
		end;
		t_dtype(n_default_type)		= "D"||put(n_default_type,1.);
		t_dstart(n_default_type)	= d_default_start_date;
		t_dend(n_default_type)		= d_default_end_date;
		t_acdate(n_default_type)	= d_ac_date;
		t_nretail(n_default_type)	= n_ind_non_retail;
		
		d_cutoff_start_date			= &dt_BBegMth.;
		d_cutoff_end_date			= &dt_BEndMth.;
		d_1st_default_start_date	= min(of t_dstart[*]);
		d_last_default_end_date		= max(of t_dend[*]);
		c_all_default_type			= catx(",",of t_dtype[*]);

		format d_: yymmdds10.;
		drop i;
		keep 
			c_default_type_d:
			d_default_start_date_d:
			d_default_end_date_d:
			d_ac_date_d: 
			d_cutoff_start_date
			d_cutoff_end_date
			d_1st_default_start_date
			d_last_default_end_date
			c_all_default_type
			key_&key. 
		;		
		if last.key_&key. then output;
	run;
	/* end of Step 3 */
	/* ************************************************************************************************* */
	/* start of Step 4 - map to latest available demographic information */
	data work.ddm_subset_detail_v3;
		merge work.ddm_subset_detail_v2 (in=a) work.key_ddm_latest_v1(in=b);
		by key_&key.;
		if a;
	run;
	/* end of Step 4 */
	/* ************************************************************************************************* */
	/* start of Step 5 - arrange ouptut fields sequence */
	data &tarlib..DDM_&key._AFTER_&st_BackMth.;
		retain 
			c_ind_mainland 
			n_svid 
			n_bk 
			n_cin 
			c_cust_id 
			c_customer_type 
			d_ac_date
			n_ind_non_retail

			c_default_type_d1 d_default_start_date_d1 d_default_end_date_d1
			c_default_type_d2 d_default_start_date_d2 d_default_end_date_d2
			c_default_type_d3 d_default_start_date_d3 d_default_end_date_d3
			c_default_type_d4 d_default_start_date_d4 d_default_end_date_d4

			d_cutoff_start_date
			d_cutoff_end_date
			d_1st_default_start_date
			d_last_default_end_date
			c_all_default_type
			key_&key. 
			d_ac_date_d: 
			;
			set work.ddm_subset_detail_v3;
			label n_ind_non_retail = "Exclude individuals or IDtype 1 or 92 are HKID";
			label c_default_type_d1 = "D1 Type";
			label c_default_type_d2 = "D2 Type";
			label c_default_type_d3 = "D3 Type";
			label c_default_type_d4 = "D4 Type";
			label d_default_start_date_d1 = "D1 Fdate";
			label d_default_start_date_d2 = "D2 Fdate";
			label d_default_start_date_d3 = "D3 Fdate";
			label d_default_start_date_d4 = "D4 Fdate";
			label d_default_end_date_d1 = "D1 Tdate";
			label d_default_end_date_d2 = "D2 Tdate";
			label d_default_end_date_d3 = "D3 Tdate";
			label d_default_end_date_d4 = "D4 Tdate";
			label d_ac_date_d1 = "D1 Inc_Acdate";
			label d_ac_date_d2 = "D2 Inc_Acdate";
			label d_ac_date_d3 = "D3 Inc_Acdate";
			label d_ac_date_d4 = "D4 Inc_Acdate";
			label d_cutoff_start_date = "Cutoff Start Date";
			label d_cutoff_end_date = "Cutoff End Date";
			label d_1st_default_start_date = "First Default Start Date among 4 types";
			label d_last_default_end_date = "Last Default End Date among 4 types";
			label c_all_default_type = "Default_type";
			%if %UPCASE(&key.)=CIN %then %do;
			label key_cin = "CIN";
			%end;	
			%if %UPCASE(&key.)=ID %then %do;
			label key_id = "bank + id";
			%end;	
	run;
	proc sort data=&tarlib..DDM_&key._AFTER_&st_BackMth.; 
		by c_ind_mainland key_&key.; 
	run;
%end;
%mend;

%exportlog(ind=START, path=&dir_log., name=DDM_&st_RptMth._CINID_MART_&st_cltinfo.);
%gen_DDM_CINID_Level(key=cin, tarlib=mart_f);
%gen_DDM_CINID_Level(key=id, tarlib=mart_f);
%exportlog(ind=STOP);


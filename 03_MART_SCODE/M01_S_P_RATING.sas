%macro gen_SP_MART(stdte=);
%if %sysfunc(exist(MART_F.SP_RATING_&stdte.)) %then %do;
	%put ---- NCB LOG ---- : !! MART_F.SP_RATING_&stdte. is already existed.;
%end;
%else %do;
	/* ************************************************************************************************* */
	/* STEP 1.0: map the s&p entity id  to raid */
	/* ************************************************************************************************* */
	%let dte = %SYSFUNC(intnx(MONTH,%SYSFUNC(inputn(&stdte.01,yymmdd8.)),0,END));
	proc sort data=raw_oth.rmp_sp_mapping out=work.sp_map(keep=SP_Entity_ID RMP_id_type RMP_id_number);
		where effect_dt_from <= &dte. <= effect_dt_to;
		by SP_Entity_ID;
	run;

	/* ************************************************************************************************* */
	/* STEP 2.1: Integate all the S&P rating information by different issue date per each entity id */
	/* ************************************************************************************************* */
	proc sort data=sashelp.vtable out=work.join_sp_list; 
		where libname = "RAW_S_P" and input(substr(memname,16,6)||"01",yymmdd8.) le &dt_RptMth.; 
		by libname; 
	run;
	data _null_;
		file "&dir_tmp./join_sp&st_TS..dat" lrecl=65535;
		set work.join_sp_list end=eof;
		by libname;
		ds = catx(".",libname,memname);
		if first.libname then put "data work.join_sp_all; set ";
		put ds;
		if last.libname then put "  ; run;";
	run;
	%include "&dir_tmp./join_sp&st_TS..dat"  /source2;

	data work.join_sp_all_v1;
		set work.join_sp_all;
		array navar {*} _numeric_;
		array cavar {*} _character_;
		sp_entity_name			= cavar(1);
		sp_entity_id			= cavar(3);
		sp_issuer_lt_fc_rating	= cavar(6);
		sp_issuer_lt_fc_date_from = navar(1);
		keep sp_:;
	run;
	proc sort data=work.join_sp_all_v1 out=work.join_sp_all_v2 nodupkey; 
		by sp_entity_id descending sp_issuer_lt_fc_date_from; 
	run;

	data work.join_sp_all_v3;
		set work.join_sp_all_v2;
		by sp_entity_id;
		sp_issuer_lt_fc_date_to=lag(sp_issuer_lt_fc_date_from);
		if first.sp_entity_id then sp_issuer_lt_fc_date_to='31Dec9999'd; 
		format sp_issuer_lt_fc_date_from sp_issuer_lt_fc_date_to yymmdds10.;
	run;
	proc sort data=work.join_sp_all_v3 out=work.join_sp_all_v3a; 
		by sp_entity_id ;
		where sp_issuer_lt_fc_date_from <= &dte. <= sp_issuer_lt_fc_date_to;
	run;
	data work.sp_rmp_all;
		merge work.join_sp_all_v3a(in=a) work.sp_map(in=b);
		by sp_entity_id;
		length sasflag_noinfo $20;
		if a and not b then sasflag_noinfo = 'No RMP vlookup';
		if b and not a then sasflag_noinfo = 'No S&P vlookup';
	run;
	proc sort data=work.sp_rmp_all out=MART_F.SP_RATING_&stdte.;
		by RMP_id_type RMP_id_number;
	run;
%end;
%mend;

%exportlog(ind=START, path=&dir_log., name=SP_&st_RptMth._MART_&st_cltinfo.);
%gen_SP_MART(stdte=&st_BackMth.);
%gen_SP_MART(stdte=&st_RptMth.);
%exportlog(ind=STOP);




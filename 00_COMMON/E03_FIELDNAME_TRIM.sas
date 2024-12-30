%macro trim_SRC_FIELDNAME(proposed_meta=, realtime_meta=, final_meta=, txtfile=, dt_dSpecific= ,st_dSpecific= );
	/* ************************************************************************************************ */
	/* Filter the proper field name description within effective date range.							*/
	/* ************************************************************************************************ */

	%if &st_dSpecific. ne  %then %let dt_dSpecific=%SYSFUNC(intnx(MONTH,%SYSFUNC(inputn(&st_dSpecific.01,yymmdd8.)),0,END));
	%if &dt_dSpecific. eq  %then %let dt_dSpecific=&dt_Rptmth.;

	data work.v_metainfo /view=work.v_metainfo;
		length orig_name $255;
		%if &txtfile. ne  %then %do;
		set &proposed_meta.(where=(effect_dt_from <= &dt_dSpecific. <= effect_dt_to and remark contains "&txtfile."));
		%end;
		%else %do;
		set &proposed_meta.(where=(effect_dt_from <= &dt_dSpecific. <= effect_dt_to));
		%end;
		metainfo_seq=_n_;
	run;
	proc sort data=work.v_metainfo out=work.proposed_metainfo; 
		by orig_name metainfo_seq; 
	run;
	proc sql noprint;
		alter table &realtime_meta. modify orig_name char(255) ;
	quit;
	proc sort data=&realtime_meta. out=work.realtime_metainfo(keep=orig_name machine_name realtime_seq); 
		by orig_name realtime_seq;
	run;
	/* ************************************************************************************************ */
	/* Remind: 																							*/
	/* if n_flag_metainfo_only = 1 or n_flag_srcfile_only =1, it must be raised to exceptional report 	*/
	/* ************************************************************************************************ */
	data work.metainfo_v1;
		merge work.proposed_metainfo(in=a) work.realtime_metainfo(in=b);
		by orig_name;
		if a or b;
		if a and not b then n_ind_proposed_meta_only =1;
		if b and not a then n_ind_realtime_meta_only =1;
		
		/* check the same orig fieldname and designated fieldname but not different type*/
		length machine_duplicate_name $255.;
		if (lowcase(strip(machine_name))=lowcase(strip(SAS_name))) and (strip(orig_type) ne strip(SAS_type)) then do;
			machine_duplicate_name = strip(machine_name); /* the src fieldname into tmp variable and rename it */
			machine_name = "_"||put(realtime_seq,z5.);	/* assign unique fieldname during updating meta */
		end;
	run;
	proc sort data=work.metainfo_v1; by orig_name metainfo_seq; run;
	data work.metainfo_v1;
		set work.metainfo_v1;
		by orig_name metainfo_seq;
		if first.metainfo_seq ne last.metainfo_seq then n_err_targetmeta=1;
	run;
	proc sort data=work.metainfo_v1; by orig_name realtime_seq; run;
	data work.metainfo_v1;
		set work.metainfo_v1;
		by orig_name realtime_seq;
		if first.realtime_seq ne last.realtime_seq then n_err_sourcemeta=1;
	run;
	proc sort data=work.metainfo_v1 out=&final_meta.;
		by metainfo_seq realtime_seq;
	run;
%mend;
/*
%trim_SRC_FIELDNAME(proposed_meta=raw.meta_bpe_rsme003_initial, realtime_meta=work.rtmeta_bpe_rsme003_201507_201512, final_meta=stg.rtmeta_bpe_rsme003_201507_201512);
*/

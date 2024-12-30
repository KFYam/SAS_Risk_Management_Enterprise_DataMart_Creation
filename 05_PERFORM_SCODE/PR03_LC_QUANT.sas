/* ************************************************************************************* */
/* For LC PD */
/* ************************************************************************************* */

%macro gen_LC_QUANT(stdte=, rmp=);
	
	%let TBL	= mart.&RMP._ALL_&stdte.;
	%let dtdte	= %SYSFUNC(intnx(MONTH,%SYSFUNC(inputn(&stdte.01,yymmdd8.)),0,END));

	data _tmp;
		set &tbl.;
		where missing(sasflag_dq_excluded);
		if missing(sasflag_ddm_ind) then sasflag_ddm_ind= 0;
	run;
	proc freq data=_tmp;
	title 'LC AR result - &stdte. and 1603 default list';
	tables sasflag_ddm_ind*c_final_rating / measures alpha=0.01 nocol norow nopercent;
	test SMDCR;
	run; 

%mend;
%gen_LC_QUANT(stdte=&st_BackMth.,	rmp=RMP2BIO2);



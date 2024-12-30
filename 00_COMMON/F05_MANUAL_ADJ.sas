/* Macro for manual adjustment adoption */

%macro update_ManualAdj(table_name=, src=, tar= ,st_date=, mode=);
	%if &st_date. eq  %then %do;	
		%let dte = &dt_RptMth.; 
	%end;
	%else %do;						
		%let dte = %SYSFUNC(intnx(MONTH,%SYSFUNC(inputn(&st_date.01,yymmdd8.)),0,END)); 
	%end;

	data _manual_;
		file "&dir_tmp./ea_&table_name._U&st_TS..dat" lrecl=65535;
		set raw_oth.manual_adj;
		if effect_dt_from  <= &dte. <= effect_dt_to;
		if table_name  		= "&table_name.";
		if mode 			= "U";
		put detail;
	run;
	%let u_cnt = 0;
	proc sql noprint; select count(1) into :u_cnt from _manual_; quit;
	%if %eval(&u_cnt.) gt 0 %then %do;
		data &tar.;
			set &src.;
			%include "&dir_tmp./ea_&table_name._U&st_TS..dat" /source2;
		run;
	%end;
	
%mend;
/*
%gen_ManualAdj(dte="28Feb2013"d, tbl=BBR_Output, mode=U);
%update_ManualAdj(table_name=BBR_Output, src=raw_bbr.bbr_output_201411, tar=test, st_date=201411);
*/

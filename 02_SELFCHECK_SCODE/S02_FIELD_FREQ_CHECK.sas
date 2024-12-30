%macro SC_DataAssess(dte=,lib=,src=,out=);
	/* %let dte=201605; %let lib=RAW_BPE; %let src=BPE_RSME_COLLAPPO; */
	%let tbl=&src._&dte.; 
	%let rawtblobs = 0;
	%put ==== [NCB Log] ====: Table name is &tbl.;

	proc contents data=&lib..&tbl. memtype=data out=_tTblInfo noprint;run;	
	data _tTblInfo1 (rename=(memname=table_name varnum=seq name=variable type_new=type format_new=format));
		set _tTblInfo;
		memname = tranwrd(memname,"_&dte.","");
		length type_new $4 format_new $20;
		if type=1 then type_new="Num"; else type_new="Char";
		if not missing(format) then format_new=cats(format, put(formatl,best32.),".");
		keep libname memname name type_new varnum label length format_new;
	run;
	proc sort data=WORK._tTblInfo1; by seq;run;
	proc sql noprint; select count(1) into :rawtblobs from &lib..&tbl.; quit;
	%if &rawtblobs. gt 0 %then %do;
		/* ******************************************************************************************** */
		/* ******************************************************************************************** */
		/* STEP 1 - start : Check all numeric fields */
		ods listing close;
		ods output summary=_tNumStat;
		proc means data=&lib..&tbl. stackods N Nmiss mean std min p1 p5 p10 p25 p50 p75 p90 p95 p99 max  MAXDEC=8; run;
		ods output close;
		data WORK._tNumStat_v1(keep=variable value n_:);
			length value $255 NObs 8;
			set _tNumStat;
			NObs=sum(n,nmiss);
			array avar {*} _numeric_;
			do i=1 to dim(avar);
				value=vname(avar(i));
				n_&dte.=avar(i);
				output;
			end;
		run;
		/* STEP 1 - finished : Check all numeric fields */
		/* ******************************************************************************************** */
		/* ******************************************************************************************** */
		/* STEP 2 - start : check character field individully */
		data _tTblInfo_char; set _tTblInfo1;where type="Char"; run;
		proc sql noprint; select count(1) into :char_var_cnt from _tTblInfo_char; quit;
		/*	%put char_var_cnt; */
		%do i=1 %to &char_var_cnt.;
			data _null_;
				set _tTblInfo_char(firstobs=&i. obs=&i.);
				call symput("var",strip(variable));
			run;	
			%put &var.;
			proc freq data=&lib..&tbl.; table &var. ; ods output OneWayFreqs=_tfreq; 
			proc freq data=&lib..&tbl.; table &var. /missing; ods output OneWayFreqs=_tfreq_w_miss; run;
			ods output close;
			/* ******************************************************************************************** */
			/* STEP 2.1 : check any pattern; if no of distinct > 200, or max of percent < 5%, 				*/
			/* it is classified as NO-PATTERN (!!! Not including missing value) 							*/
			proc sql noprint; select count(&var.), int(max(max(percent),0)) into :cnt,:max_pct from _tfreq; quit;
			%put &cnt.; %put &max_pct.;
			%if %eval(&cnt.) gt 200 or %eval(&max_pct.) lt 5 %then %do;
				proc sql noprint;
					create table WORK._tCharStat&i. as
					select "&var." as variable, cat('<Length of Text> ',strip(put(char_len,best32.))) as value, count(char_len) as n_&dte. 
					from (select lengthn(&var.) as char_len from &lib..&tbl.)
					group by char_len 
					order by char_len;
				quit;
			%end;
			%else %do;
			/* ******************************************************************************************** */
			/* STEP 2.2 : show attibute in char field														*/
				data WORK._tCharStat&i. (rename=(Table=variable Frequency=n_&dte. ));
					length value $400;
					set _tfreq_w_miss;
					Table=strip(tranwrd(Table,"Table",""));
					if missing(value) and missing(&var.) then value="<Missing>";
					if missing(value) and missing(strip(&var.)) then value="<Blank>";
					if missing(value) then value=&var.;
					keep Table value Frequency;
				run;
			%end;
		%end;	
		/* STEP 2.3 : concatenate all the char freq information */
		data WORK._tStat_all;
			length variable value $400 ;
			set WORK._tNumStat_v1
			%do i=1 %to &char_var_cnt.;	WORK._tCharStat&i. %end;
			;
			label n_&dte.="&dte.";
			format n_&dte. best32.;
			tmp_seq=_N_;
		run;
		proc sql noprint;
			create table &out.(drop=tmp_seq) as
			select a.*, b.value, b.n_&dte., b.tmp_seq, 
			case when	a.type="Num" and not missing(a.format) and 
						b.value not in ("N","NMiss","NObs","StdDev") 
				 then strip(putn(b.n_&dte.,a.format)) 
				 else strip(put(b.n_&dte.,best32.))
			end as c_&dte.
			from WORK._tTblInfo1 a left join WORK._tStat_all b
			on a.variable = b.variable
			order by a.seq, b.tmp_seq;
		quit; 
		proc datasets lib=work; delete _t:; quit;
		ods listing;
	%end;
	%else %do;
		data &out.;	
			length LIBNAME $8 table_name $32 variable $32 value $400; 
			delete ;
		run;
	%end;
%mend;

%macro SC_LoopAssess(lib=, outlib=, table_prefix= ,keephistory=);
	/*	%let lib=RAW_BPE; 
		%let lib=RTL_CAR; 
		%let outlib=WORK; 
		%let table_prefix = CAR_RESULT; */
	proc sort data=sashelp.vstable out= _tmp; where libname=upcase("&lib."); by memname; run;
	data work.assess_tablelist;
		set _tmp;
		length t_yyyymm t_yyyymm2nd t_remaintext $32 t_date t_date2nd sasflag_excluded 8;
		t_yyyymm=scan(memname,-1,'_');

		if not missing(t_yyyymm) and missing(compress(t_yyyymm," 0123456789")) then do;
			if lengthn(t_yyyymm) eq 6 then t_date=input(t_yyyymm,yymmn6.);
			if lengthn(t_yyyymm) eq 4 then t_date=input(t_yyyymm,?? yymmn4.);
		end;

		if length(memname)-length(t_yyyymm)-1 > 0 then t_remaintext=substr(memname,1, length(memname)-length(t_yyyymm)-1);

		t_yyyymm2nd=scan(t_remaintext,-1,'_');

		if not missing(t_yyyymm2nd) and missing(compress(t_yyyymm2nd," 0123456789")) then do;
			if lengthn(t_yyyymm2nd) eq 6 then t_date2nd=input(t_yyyymm2nd,yymmn6.);
			if lengthn(t_yyyymm2nd) eq 4 then t_date2nd=input(t_yyyymm2nd,?? yymmn4.);
		end;

		if missing(sasflag_excluded) and find(upcase(memname),"META") > 0   then sasflag_excluded=1;
		if missing(sasflag_excluded) and missing(t_date)   			then sasflag_excluded=2;
		if missing(sasflag_excluded) and not missing(t_date2nd)   	then sasflag_excluded=3;

		%if &table_prefix. ne %then %do;
		/*20170407 : Wing : change 1 line : use exact checking (e.g. I want run for CAR_RESULT_yymm but not CAR_RESULT_AGGR_yymm) */
		/*if missing(sasflag_excluded) and find(memname,"&table_prefix._") <= 0 then sasflag_excluded=4;*/ /* modified on 20170118, must add '_' after the table_prefix to distinguish similar tables, such as RMP2BIOLCM and RMP2BIOLCMB case */
		if missing(sasflag_excluded) and strip(upcase(t_remaintext)) ne upcase("&table_prefix.") then sasflag_excluded=4;
		%end;
	run;
	proc sort data=work.assess_tablelist out=work.assess_tablelist_v1; by t_remaintext t_yyyymm; where missing(sasflag_excluded); run;
		
	data WORK._SCLOOPLIST;
		file "&dir_tmp./sc_loop&st_TS..dat" lrecl=65535;
		set work.assess_tablelist_v1 end=eof;
		by t_remaintext;

		length _mergesubset_ _targetfile_ $255;
		_mergesubset_=cats("WORK._SCLOOP",put(_N_,best32.));
		_targetfile_= cats("&outlib..METASC_",t_remaintext);

		if first.t_remaintext then do;
			put "data " _targetfile_ "(drop=n_:);";
			put "  merge ";
			%if &keephistory. eq YES %then %do; 
				put _targetfile_ ;	
			%end;
		end;
		put _mergesubset_;
		if last.t_remaintext then do;
			put "  ;";
			put "  by libname table_name variable value;";
			put "run;";
			put "  data _null_; call sleep(3); run;";
		end;
	run;

	proc sql noprint; select count(1) into :loop_cnt from WORK._SCLOOPLIST; quit;
	%if &table_prefix. ne %then %do;
		%put ==== [NCB Log] ====: Table name with the prefix of &table_prefix. would only be selected for checking;
	%end;
	%put ==== [NCB Log] ====: Start looping all the tables for assessment indivdually;
	%do j=1 %to &loop_cnt.;
		data _null_;
			set WORK._SCLOOPLIST(firstobs=&j. obs=&j.);
			call symput("dte",t_yyyymm);
			call symput("src",t_remaintext);
		run;
		%put ==== [NCB Log] ====: Start processing the loop no=&j., table name=&src., table suffix=&dte. ;
		%SC_DataAssess(dte=&dte., lib=&lib. ,src=&src., out=WORK._SCLOOP&j.);
		proc sort data=WORK._SCLOOP&j./*(drop=n_:)*/; by libname table_name variable value; run;
		%put ==== [NCB Log] ====: Completed processing the loop no=&j., table name=&src., table suffix=&dte. ;
	%end;
	%put ==== [NCB Log] ====: Completed looping all the tables for assessment indivdually;
	%put ==== [NCB Log] ====: Start merging all the resultsets into single table;
	%include "&dir_tmp./sc_loop&st_TS..dat" /source2;
	%put ==== [NCB Log] ====: Completed merging all the resultsets into single table;
	proc datasets lib=work; delete _SCLOOP: _tmp:; quit;
%mend;

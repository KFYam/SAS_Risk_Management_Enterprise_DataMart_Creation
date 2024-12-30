%macro check_dataowner(lib=, out=);
	%let lib=raw_bpe;
	%let refpath = %unquote(%sysfunc(pathname(&lib.)));
	filename DS_List pipe "ls -l &refpath.|grep sas7bdat"; * where directory is the path to your library;
	data &out.;
		infile DS_List;
		length owner $8 filname $50 memname $32;
		input;
		/*a=_infile_;*/
		owner = scan(_infile_,3," ");
		filname = scan(_infile_,9," ");
		memname = scan(filname,1,".");
		drop filname;
	run;
%mend;


%macro gen_metainfo(raw=, ds_xlsmeta=);
	proc sql noprint;
		create table _tmp1_ as
			select * from sashelp.vtable where libname="RAW" and memname contains "&raw." and memname not contains "META" order by memname;
		select count(1) into :tcnt from _tmp1_;
	run;
	%if &msuf.=INITIAL %then %do;
		data _null_;
			set _tmp1_;
			call symput("rtmeta"||strip(put(_N_,32.)),strip(memname));
		run;
	%end;
	%else %do;
		%let tcnt=1;
		%let rtmeta1=&raw._&st_Rptmth.;	
	%end;
	%do i=1 %to &tcnt.;
		%let metacnt_x=0;
		/*%put ==========> test===> &&rtmeta&i.;*/
		proc sql noprint;
			select count(1) into :metacnt_x from raw.&ds_xlsmeta. where effect_dt_from <= &dt_Rptmth. <= effect_dt_to and remark contains "&raw.";
		quit;	
		data tmp(keep=file_r file_m metacnt_r metacnt_x field_meta_only field_rtmeta_only);
			length field_meta_only field_rtmeta_only $1000;
			length file_r file_m  $32;
			length metacnt_r metacnt_x  8;

			retain field_meta_only ''	field_rtmeta_only '' metacnt_r 0;
			set stg.rtmeta_&&rtmeta&i.. end=eof;

			file_r="&&rtmeta&i..";
			file_m="&ds_xlsmeta."; 
			metacnt_x=&metacnt_x.;

			if n_ind_proposed_meta_only =1	then field_meta_only=catx(',',field_meta_only,orig_name);
			if n_ind_realtime_meta_only =1	then field_rtmeta_only=catx(',',field_rtmeta_only,orig_name);
			if realtime_seq ne . 			then metacnt_r=sum(metacnt_r,1);

			if eof then output;
		run;
		
		data t_&raw.&i.;
			retain file_r file_m metacnt_r metacnt_x metacnt_diff field_meta_only field_rtmeta_only;
			length file_r file_m $32;
			length field_meta_only field_rtmeta_only $1000;
			label file_r="Name of source file";
			label file_m="Name of excel meta file";
			label metacnt_r="No of fields in source file";
			label metacnt_x="No of fields in excel meta file";
			label metacnt_diff="No of fields (diff)";
			label field_meta_only="Expected fields NOT FOUND in source file";
			label field_rtmeta_only="UNEXPECTED fields in source file";
			set tmp ; 
			metacnt_diff = sum(metacnt_r,-metacnt_x);
			if missing(field_meta_only) 	then field_meta_only="nil";
			if missing(field_rtmeta_only) 	then field_rtmeta_only="nil";
			format _numeric_ comma32.;
		run;
		data _null_;
			file "&dir_tmp./t_rpt1&st_TS..dat" mod;
			put "t_&raw.&i.";
		run;
	%end;
%mend;
%macro gen_metaSummary(datasrc=);
	data _null_;
		file "&dir_tmp./t_rpt1&st_TS..dat" lrecl=65535;
		put "data stg_oth.rpt_metainfo_dim1_&msuf.;";
		put "set ";
	run;
	%if &datasrc. = RMP %then %do;
		%gen_metainfo(raw=RMP2BIO2, 		ds_xlsmeta=META_RMP_LCM_&msuf.);
		%gen_metainfo(raw=RMP2BIOLCM, 		ds_xlsmeta=META_RMP_LCM_&msuf.);
		%gen_metainfo(raw=RMP2BIO2B, 		ds_xlsmeta=META_RMP_LCMB_&msuf.);
		%gen_metainfo(raw=RMP2BIOLCMB, 		ds_xlsmeta=META_RMP_LCMB_&msuf.);
		%gen_metainfo(raw=RMP2BIO3, 		ds_xlsmeta=META_RMP_MM_&msuf.);
		%gen_metainfo(raw=RMP2BIOMM,		ds_xlsmeta=META_rmp_mm_&msuf.);
		%gen_metainfo(raw=RMP2BIO3B,		ds_xlsmeta=META_rmp_mmb_&msuf.);
		%gen_metainfo(raw=RMP2BIOMMB,		ds_xlsmeta=META_rmp_mmb_&msuf.);
		%gen_metainfo(raw=RMP2BIO5A,		ds_xlsmeta=META_rmp_reimsa_&msuf.);
		%gen_metainfo(raw=RMP2BIOREIMSA,	ds_xlsmeta=META_rmp_reimsa_&msuf.);
		%gen_metainfo(raw=RMP2BIO5B,		ds_xlsmeta=META_rmp_reimsb_&msuf.);
		%gen_metainfo(raw=RMP2BIOREIMSB,	ds_xlsmeta=META_rmp_reimsb_&msuf.);
		%gen_metainfo(raw=RMP2BIO5C,		ds_xlsmeta=META_rmp_reimsc_&msuf.);
		%gen_metainfo(raw=RMP2BIOREIMSC,	ds_xlsmeta=META_rmp_reimsc_&msuf.);
		%gen_metainfo(raw=RMP2BIO7,			ds_xlsmeta=META_rmp_redi_&msuf.);
		%gen_metainfo(raw=RMP2BIOREDI,		ds_xlsmeta=META_rmp_redi_&msuf.);
		%gen_metainfo(raw=RMP2BIO7B,		ds_xlsmeta=META_rmp_redib_&msuf.);
		%gen_metainfo(raw=RMP2BIOREDIB,		ds_xlsmeta=META_rmp_redib_&msuf.);
		%gen_metainfo(raw=RMP2BIO10A,		ds_xlsmeta=META_rmp_ofa_&msuf.);
		%gen_metainfo(raw=RMP2BIOOFA,		ds_xlsmeta=META_rmp_ofa_&msuf.);
		%gen_metainfo(raw=RMP2BIO10B,		ds_xlsmeta=META_rmp_ofb_&msuf.);
		%gen_metainfo(raw=RMP2BIOOFB,		ds_xlsmeta=META_rmp_ofb_&msuf.);
		%gen_metainfo(raw=RMP2BIO10C,		ds_xlsmeta=META_rmp_ofc_&msuf.);
		%gen_metainfo(raw=RMP2BIOOFC,		ds_xlsmeta=META_rmp_ofc_&msuf.);
		%gen_metainfo(raw=RMP2BIO9A,		ds_xlsmeta=META_rmp_pfa_&msuf.);
		%gen_metainfo(raw=RMP2BIOPFA,		ds_xlsmeta=META_rmp_pfa_&msuf.);
		%gen_metainfo(raw=RMP2BIO9B,		ds_xlsmeta=META_rmp_pfb_&msuf.);
		%gen_metainfo(raw=RMP2BIOPFB,		ds_xlsmeta=META_rmp_pfb_&msuf.);
		%gen_metainfo(raw=RMP2BIO9C,		ds_xlsmeta=META_rmp_pfc_&msuf.);
		%gen_metainfo(raw=RMP2BIOPFC,		ds_xlsmeta=META_rmp_pfc_&msuf.);
		%gen_metainfo(raw=RMP2BIO12A,		ds_xlsmeta=META_rmp_nbfiinsa_&msuf.);
		%gen_metainfo(raw=RMP2BIONBFIINSA,	ds_xlsmeta=META_rmp_nbfiinsa_&msuf.);
		%gen_metainfo(raw=RMP2BIO12B,		ds_xlsmeta=META_rmp_nbfiinsb_&msuf.);
		%gen_metainfo(raw=RMP2BIONBFIINSB,	ds_xlsmeta=META_rmp_nbfiinsb_&msuf.);
		%gen_metainfo(raw=RMP2BIO13A,		ds_xlsmeta=META_rmp_nbfiseca_&msuf.);
		%gen_metainfo(raw=RMP2BIONBFISECA,	ds_xlsmeta=META_rmp_nbfiseca_&msuf.);
		%gen_metainfo(raw=RMP2BIO13B,		ds_xlsmeta=META_rmp_nbfisecb_&msuf.);
		%gen_metainfo(raw=RMP2BIONBFISECB,	ds_xlsmeta=META_rmp_nbfisecb_&msuf.);
		%gen_metainfo(raw=RMP2BIO11A,		ds_xlsmeta=META_rmp_banka_&msuf.);
		%gen_metainfo(raw=RMP2BIOBANKA,		ds_xlsmeta=META_rmp_banka_&msuf.);
		%gen_metainfo(raw=RMP2BIO11B,		ds_xlsmeta=META_rmp_bankb_&msuf.);
		%gen_metainfo(raw=RMP2BIOBANKB,		ds_xlsmeta=META_rmp_bankb_&msuf.);
		%gen_metainfo(raw=RMP2BIO11C,		ds_xlsmeta=META_rmp_bankc_&msuf.);
		%gen_metainfo(raw=RMP2BIOBANKC,		ds_xlsmeta=META_rmp_bankc_&msuf.);
		%gen_metainfo(raw=RMP2BIO11D,		ds_xlsmeta=META_rmp_bankd_&msuf.);
		%gen_metainfo(raw=RMP2BIOBANKD,		ds_xlsmeta=META_rmp_bankd_&msuf.);
		%gen_metainfo(raw=RMP2BIO11E,		ds_xlsmeta=META_rmp_banke_&msuf.);
		%gen_metainfo(raw=RMP2BIOBANKE,		ds_xlsmeta=META_rmp_banke_&msuf.);
		%gen_metainfo(raw=RMP2BIO11F,		ds_xlsmeta=META_rmp_bankf_&msuf.);
		%gen_metainfo(raw=RMP2BIOBANKF,		ds_xlsmeta=META_rmp_bankf_&msuf.);
	%end;
	%if &datasrc. = BPE %then %do;
		%gen_metainfo(raw=BPE, 				ds_xlsmeta=META_BPE_&msuf.);
		%gen_metainfo(raw=BPE_RSME003,		ds_xlsmeta=META_BPE_RSME003_&msuf.);
		%gen_metainfo(raw=BPE_RSME004,		ds_xlsmeta=META_BPE_RSME004_&msuf.);
		%gen_metainfo(raw=BPE_RSMEPOOL,		ds_xlsmeta=META_BPE_RSMEPOOL_&msuf.);
		%gen_metainfo(raw=BPE_RSME_CollBasic,ds_xlsmeta=META_BPE_RSME_CollBasic_&msuf.);
		%gen_metainfo(raw=BPE_RSME_CollAppo, ds_xlsmeta=META_BPE_RSME_CollAppo_&msuf.);
		%gen_metainfo(raw=BPE_RSME_GuaBasic, ds_xlsmeta=META_BPE_RSME_GuaBasic_&msuf.);
	%end;
	%if &datasrc. = BBR %then %do;
		%gen_metainfo(raw=BBR_OUTPUT, 		ds_xlsmeta=META_BBR_OUTPUT_&msuf.);
	%end;

	data _null_;
		file "&dir_tmp./t_rpt1&st_TS..dat" mod;
		put ";";
		put "run;";
	run;
	%include "&dir_tmp./t_rpt1&st_TS..dat" /source2;
	proc datasets lib=work noprint;
		delete t_:;
	run;
	data _null_;
   		slept=sleep(3); /*sleep 3 sec for delete records */
	run;
%mend;
%macro countMiss_inField(datasrc=);
	/* %let datasrc=BBR; */
	data table_obsvar_info;
		set sashelp.vtable;
		where libname = "STG" and substr(memname,1,3)="&datasrc.";
		st_yymm		=reverse(substr(reverse(strip(memname)),1,6));
		srcname		= tranwrd(memname,"_"||strip(st_yymm),"");
		keep libname srcname memname nobs nvar st_yymm;
	run;
	proc sort data=table_obsvar_info; by st_yymm memname; run;
	proc sql noprint; select count(1) into :cntx from table_obsvar_info; quit;

	%do i=1 %to &cntx.;
		data _null_;
			set table_obsvar_info(firstobs=&i. obs=&i.);
			tbl=strip(libname)||"."||strip(memname);
			call symput("tbl&i.",tbl);
		run;
		/* count missing value in all fields of a table */ 
		data t1(keep=x); 
			set &&tbl&i.; 
			x=0; 
			x=cmiss(of _all_);	
		run;
		proc sql noprint; 
			select sum(x) into :cmiss&i. from t1;	
		quit;
	%end;
	data stg_oth.rpt_metainfo_dim2_&msuf.;
		set table_obsvar_info;
		cnt_miss 		= input(symget( "cmiss"||strip(put(_n_,32.))),best32.);
		count			= nobs*nvar;
		avg_colprow 	= (nobs*nvar - cnt_miss)/nobs;
	run;
%mend;
%macro suffix_setting();
	%if &flag_initial. = YES %then 
		%let msuf=INITIAL; 
	%else 
		%let msuf=&st_RptMth.;
%mend;

/* *************************************************************************************************************************** */
/* START - Expected the files of stg_oth.rpt_metainfo_dim1_&st_Rptmth. and stg_oth.rpt_metainfo_dim2_&st_Rptmth. are generated */
/* Below variables are for debug purpose */
/* %let raw=RMP2BIO2; 	%let rtmeta1=&raw._200912;	%let ds_xlsmeta=META_RMP_LCM_&msuf.; */
%macro gen_SelfCheck_Report_MetaSummary(src=);
	%local msuf;
	%suffix_setting;
	%let src=BPE;
	%gen_metaSummary(datasrc=&src.);
	%countMiss_inField(datasrc=&src.);

	/*x "cp &dir_rptdq./_template_Rpt_MetaInfo_Summary.xls &dir_rptdq./Rpt_MetaInfo_Summary_&st_Rptmth..xls";*/
	proc export 
		data=stg_oth.rpt_metainfo_dim1_&msuf. 
		outfile="&dir_rptsc./SelfChecking_Data_&src._&msuf..xlsx" dbms=xlsx replace;
 	  	sheet='Source01';
	run;

	proc export 
		data=stg_oth.rpt_metainfo_dim2_&msuf. 
		outfile="&dir_rptsc./SelfChecking_Data_&src._&msuf..xlsx" dbms=xlsx replace;
 	  	sheet='Source02';
	run;
	/*
	ods tagsets.ExcelXP file="&dir_rptsc./SelfChecking_Data_&src._&msuf..xls" ;
	ods tagsets.ExcelXP options(sheet_name='Source01' );
	proc print data=stg_oth.rpt_metainfo_dim1_&msuf. noobs; run;

	ods tagsets.ExcelXP options(sheet_name='Source02');
	proc print data=stg_oth.rpt_metainfo_dim2_&msuf. noobs; run;
	ods tagsets.ExcelXP close;
	*/
%mend;
/*
%gen_SelfCheck_Report_MetaSummary(src=RMP);
*/

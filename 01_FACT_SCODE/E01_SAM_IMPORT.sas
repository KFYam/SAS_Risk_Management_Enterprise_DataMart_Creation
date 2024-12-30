/* ************************************************************************************************ */
/* To import the source file in SAS dataset in characters format for avoiding information loss.		*/
/* The SAS dataset in characters format is saved in libname "STG", we will have regular				*/
/* housekeeping on every 2 years in this libname "STG".												*/
/* ************************************************************************************************ */
/* Reference: https://communities.sas.com/t5/General-SAS-Programming/Creator-of-table/td-p/176453	*/

%macro export_sam_csv();
	filename samlist pipe "ls -l &dir_sam.|grep -i '.tar.gz'"; * where directory is the path to your library;
	data sam_fileinfo;
		infile samlist;
		length owner $8 filname $50;
		input;
		owner 	= scan(_infile_,3," ");
		filname = strip(scan(_infile_,9," "));
		suffix	= scan(filname,-2,"_");
		prefix	= scan(filname,1,"_");

		dt_ymd	= input(suffix, yymmdd8.);
		lagmth	= intnx("MONTH",dt_ymd,-1,"END");
		st_lmth	= put(lagmth,yymmn6.);
		foldname=catt(prefix,"_",suffix);

		length unixcommand $255;
		unixcommand='[ ! -d ../csv_weekly/'||strip(foldname)||' ]  && mkdir -p .././csv_weekly/'||strip(foldname)||' && tar -zxvf '||strip(filname)||" --strip-components=2 -C .././csv_weekly/"||strip(foldname);
		format dt_ymd lagmth date9.;
	run;
	proc sort data=sam_fileinfo; by lagmth dt_ymd; run;
	data _null_;
		file "&dir_tmp./sam1&st_TS..dat" lrecl=65535;
		set sam_fileinfo; 
		length txt $4000;
		if _n_ = 1 then do; txt = "x 'cd &dir_sam.';"; put txt;end;
		txt='x "'||strip(unixcommand)||'";';
		put txt;
	run;
	%include "&dir_tmp./sam1&st_TS..dat" /source2;
%mend;

%macro gen_SAM_LGD(type=,file=);
	data &file.;
		set sam_fileinfo;
		by filetype lagmth;
		if first.lagmth and filetype="&type." then output;
	run;
	data _null_;
		set &file.;
		where strip(st_lmth) = "&st_RptMth.";
		call symput("foldname",strip(foldname));
	run;
	%if &foldname. ne  %then %do;
		%import_InputMaster(sheet=MetaInfo_SAM_AC_DATA, 		out=raw_sam.meta_sam_acdata_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_AC_EXCEPT, 		out=raw_sam.meta_sam_acexcept_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_AC_HIS, 			out=raw_sam.meta_sam_achis_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_CASH_FLOW1, 		out=raw_sam.meta_sam_cashflow1_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_CASH_FLOW2, 		out=raw_sam.meta_sam_cashflow2_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_CF1_DELETE, 		out=raw_sam.meta_sam_cf1delete_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_CF2_DELETE, 		out=raw_sam.meta_sam_cf2delete_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_COLL_CIN, 		out=raw_sam.meta_sam_collcin_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_COLL_DATA, 		out=raw_sam.meta_sam_colldata_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_COLL_RELATE, 	out=raw_sam.meta_sam_collrelate_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_COLL_RVSVAL, 	out=raw_sam.meta_sam_collrvsval_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_COLL_VAL, 		out=raw_sam.meta_sam_collval_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_CRN_DATA, 		out=raw_sam.meta_sam_crndata_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_CRN_DELETE, 		out=raw_sam.meta_sam_crndelete_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_IPO, 			out=raw_sam.meta_sam_ipo_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_LIST, 			out=raw_sam.meta_sam_list_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_DEFAULT, 		out=raw_sam.meta_sam_default_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_DW57SCH, 		out=raw_sam.meta_sam_dw57sch_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_OLD_CIN, 		out=raw_sam.meta_sam_oldcin_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_RELATE_AC, 		out=raw_sam.meta_sam_relateac_&st_RptMth.);
		%import_InputMaster(sheet=MetaInfo_SAM_WRITE_OFF, 		out=raw_sam.meta_sam_writeoff_&st_RptMth.);

		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._ac_data_043.csv, 		orig_delim=",", sas_tblname=stg_sam.&type._acdata_&st_RptMth., 	realtime_meta=work.rtmeta_&type._acdata_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._ac_except_043.csv, 	orig_delim=",", sas_tblname=stg_sam.&type._acexcept_&st_RptMth.,realtime_meta=work.rtmeta_&type._acexcept_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._ac_his_043.csv, 		orig_delim=",", sas_tblname=stg_sam.&type._achis_&st_RptMth., 	realtime_meta=work.rtmeta_&type._achis_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._cash_flow1_043.csv, 	orig_delim=",", sas_tblname=stg_sam.&type._cashflow1_&st_RptMth., realtime_meta=work.rtmeta_&type._cashflow1_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._cash_flow2_043.csv, 	orig_delim=",", sas_tblname=stg_sam.&type._cashflow2_&st_RptMth., realtime_meta=work.rtmeta_&type._cashflow2_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._cf1_delete_043.csv, 	orig_delim=",", sas_tblname=stg_sam.&type._cf1delete_&st_RptMth., realtime_meta=work.rtmeta_&type._cf1delete_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._cf2_delete_043.csv, 	orig_delim=",", sas_tblname=stg_sam.&type._cf2delete_&st_RptMth., realtime_meta=work.rtmeta_&type._cf2delete_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._coll_cin_043.csv, 	orig_delim=",", sas_tblname=stg_sam.&type._collcin_&st_RptMth., 	realtime_meta=work.rtmeta_&type._collcin_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._coll_data_043.csv, 	orig_delim=",", sas_tblname=stg_sam.&type._colldata_&st_RptMth., 	realtime_meta=work.rtmeta_&type._colldata_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._coll_relate_043.csv, 	orig_delim=",", sas_tblname=stg_sam.&type._collrelate_&st_RptMth.,	realtime_meta=work.rtmeta_&type._collrelate_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._coll_rvsval_043.csv, 	orig_delim=",", sas_tblname=stg_sam.&type._collrvsval_&st_RptMth.,	realtime_meta=work.rtmeta_&type._collrvsval_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._coll_val_043.csv, 	orig_delim=",", sas_tblname=stg_sam.&type._collval_&st_RptMth.,		realtime_meta=work.rtmeta_&type._collval_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._crn_data_043.csv, 	orig_delim=",", sas_tblname=stg_sam.&type._crndata_&st_RptMth.,		realtime_meta=work.rtmeta_&type._crndata_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._crn_delete_043.csv, 	orig_delim=",", sas_tblname=stg_sam.&type._crndelete_&st_RptMth.,	realtime_meta=work.rtmeta_&type._crndelete_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._daily_ipo_043.csv, 	orig_delim=",", sas_tblname=stg_sam.&type._dailyipo_&st_RptMth.,	realtime_meta=work.rtmeta_&type._dailyipo_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._daily_list_043.csv, 	orig_delim=",", sas_tblname=stg_sam.&type._dailylist_&st_RptMth.,	realtime_meta=work.rtmeta_&type._dailylist_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._default_043.csv, 		orig_delim=",", sas_tblname=stg_sam.&type._default_&st_RptMth.,		realtime_meta=work.rtmeta_&type._default_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._dw57sch_043.csv, 		orig_delim=",", sas_tblname=stg_sam.&type._dw57sch_&st_RptMth.,		realtime_meta=work.rtmeta_&type._dw57sch_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._old_cin_043.csv, 		orig_delim=",", sas_tblname=stg_sam.&type._oldcin_&st_RptMth.,		realtime_meta=work.rtmeta_&type._oldcin_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._relate_ac_043.csv, 	orig_delim=",", sas_tblname=stg_sam.&type._relateac_&st_RptMth.,	realtime_meta=work.rtmeta_&type._relateac_&st_RptMth., encode=); 
		%gen_SRC_IN_CHAR(orig_src=&dir_sam./.././csv_weekly/&foldname./&type._write_off_043.csv, 	orig_delim=",", sas_tblname=stg_sam.&type._writeoff_&st_RptMth.,	realtime_meta=work.rtmeta_&type._writeoff_&st_RptMth., encode=); 

		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_acdata_&st_RptMth., 		realtime_meta=work.rtmeta_&type._acdata_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._acdata_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_acexcept_&st_RptMth., 	realtime_meta=work.rtmeta_&type._acexcept_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._acexcept_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_achis_&st_RptMth., 		realtime_meta=work.rtmeta_&type._achis_&st_RptMth.,			final_meta=stg_sam.rtmeta_&type._achis_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_cashflow1_&st_RptMth., 	realtime_meta=work.rtmeta_&type._cashflow1_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._cashflow1_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_cashflow2_&st_RptMth., 	realtime_meta=work.rtmeta_&type._cashflow2_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._cashflow2_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_cf1delete_&st_RptMth., 	realtime_meta=work.rtmeta_&type._cf1delete_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._cf1delete_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_cf2delete_&st_RptMth., 	realtime_meta=work.rtmeta_&type._cf2delete_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._cf2delete_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_collcin_&st_RptMth., 	realtime_meta=work.rtmeta_&type._collcin_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._collcin_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_colldata_&st_RptMth., 	realtime_meta=work.rtmeta_&type._colldata_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._colldata_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_collrelate_&st_RptMth., 	realtime_meta=work.rtmeta_&type._collrelate_&st_RptMth.,	final_meta=stg_sam.rtmeta_&type._collrelate_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_collrvsval_&st_RptMth., 	realtime_meta=work.rtmeta_&type._collrvsval_&st_RptMth.,	final_meta=stg_sam.rtmeta_&type._collrvsval_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_collval_&st_RptMth., 	realtime_meta=work.rtmeta_&type._collval_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._collval_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_crndata_&st_RptMth., 	realtime_meta=work.rtmeta_&type._crndata_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._crndata_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_crndelete_&st_RptMth., 	realtime_meta=work.rtmeta_&type._crndelete_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._crndelete_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_ipo_&st_RptMth., 		realtime_meta=work.rtmeta_&type._dailyipo_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._dailyipo_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_list_&st_RptMth., 		realtime_meta=work.rtmeta_&type._dailylist_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._dailylist_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_default_&st_RptMth., 	realtime_meta=work.rtmeta_&type._default_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._default_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_dw57sch_&st_RptMth., 	realtime_meta=work.rtmeta_&type._dw57sch_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._dw57sch_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_oldcin_&st_RptMth., 		realtime_meta=work.rtmeta_&type._oldcin_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._oldcin_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_relateac_&st_RptMth., 	realtime_meta=work.rtmeta_&type._relateac_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._relateac_&st_RptMth.);
		%trim_SRC_FIELDNAME(proposed_meta=raw_sam.meta_sam_writeoff_&st_RptMth., 	realtime_meta=work.rtmeta_&type._writeoff_&st_RptMth.,		final_meta=stg_sam.rtmeta_&type._writeoff_&st_RptMth.);

		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._acdata_&st_RptMth.,		srctbl=stg_sam.&type._acdata_&st_RptMth.,	tartbl=raw_sam.&type._acdata_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._acexcept_&st_RptMth.,	srctbl=stg_sam.&type._acexcept_&st_RptMth.,	tartbl=raw_sam.&type._acexcept_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._achis_&st_RptMth.,		srctbl=stg_sam.&type._achis_&st_RptMth.,	tartbl=raw_sam.&type._achis_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._cashflow1_&st_RptMth.,	srctbl=stg_sam.&type._cashflow1_&st_RptMth.,tartbl=raw_sam.&type._cashflow1_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._cashflow2_&st_RptMth.,	srctbl=stg_sam.&type._cashflow2_&st_RptMth.,tartbl=raw_sam.&type._cashflow2_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._cf1delete_&st_RptMth.,	srctbl=stg_sam.&type._cf1delete_&st_RptMth.,tartbl=raw_sam.&type._cf1delete_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._cf2delete_&st_RptMth.,	srctbl=stg_sam.&type._cf2delete_&st_RptMth.,tartbl=raw_sam.&type._cf2delete_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._collcin_&st_RptMth.,		srctbl=stg_sam.&type._collcin_&st_RptMth.,	tartbl=raw_sam.&type._collcin_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._colldata_&st_RptMth.,	srctbl=stg_sam.&type._colldata_&st_RptMth.,	tartbl=raw_sam.&type._colldata_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._collrelate_&st_RptMth.,	srctbl=stg_sam.&type._collrelate_&st_RptMth.,tartbl=raw_sam.&type._collrelate_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._collrvsval_&st_RptMth.,	srctbl=stg_sam.&type._collrvsval_&st_RptMth.,tartbl=raw_sam.&type._collrvsval_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._collval_&st_RptMth.,		srctbl=stg_sam.&type._collval_&st_RptMth.,	tartbl=raw_sam.&type._collval_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._crndata_&st_RptMth.,		srctbl=stg_sam.&type._crndata_&st_RptMth.,	tartbl=raw_sam.&type._crndata_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._crndelete_&st_RptMth.,	srctbl=stg_sam.&type._crndelete_&st_RptMth.,tartbl=raw_sam.&type._crndelete_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._dailyipo_&st_RptMth.,	srctbl=stg_sam.&type._dailyipo_&st_RptMth.,	tartbl=raw_sam.&type._dailyipo_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._dailylist_&st_RptMth.,	srctbl=stg_sam.&type._dailylist_&st_RptMth.,tartbl=raw_sam.&type._dailylist_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._default_&st_RptMth.,		srctbl=stg_sam.&type._default_&st_RptMth.,	tartbl=raw_sam.&type._default_&st_RptMth.);	
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._dw57sch_&st_RptMth.,		srctbl=stg_sam.&type._dw57sch_&st_RptMth.,	tartbl=raw_sam.&type._dw57sch_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._oldcin_&st_RptMth.,		srctbl=stg_sam.&type._oldcin_&st_RptMth.,	tartbl=raw_sam.&type._oldcin_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._relateac_&st_RptMth.,	srctbl=stg_sam.&type._relateac_&st_RptMth.,	tartbl=raw_sam.&type._relateac_&st_RptMth.);
		%update_META2SRC(metatbl=stg_sam.rtmeta_&type._writeoff_&st_RptMth.,	srctbl=stg_sam.&type._writeoff_&st_RptMth.,	tartbl=raw_sam.&type._writeoff_&st_RptMth.);
	%end;
	%else %do;
		%put ---- NCB LOG ---- : !! &type. folder as at month of &st_RptMth. does not exist.;
	%end;
%mend;

%exportlog(ind=START, path=&dir_log., name=SAM_LGD_&st_RptMth._LOAD_&st_cltinfo.);
%import_Format(src=SAM);
options nomprint;
%export_sam_csv;
%gen_SAM_LGD(type=lgd,file=_t_lgd);
%exportlog(ind=STOP);

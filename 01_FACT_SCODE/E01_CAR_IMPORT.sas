/* ************************************************************************************************ */
/* To import the source file in SAS dataset in characters format for avoiding information loss.		*/
/* The SAS dataset in characters format is saved in libname "STG", we will have regular				*/
/* housekeeping on every 2 years in this libname "STG".												*/
/* ************************************************************************************************ */

%macro gen_CAR(meta=);
%if %SYSFUNC(exist(raw_car.&meta._&st_RptMth.)) = 1 %then %do;
	%put ---- NCB LOG ---- : !! raw_car.&meta._&st_RptMth. is already existed.;
%end;
%else %do;
	%if %sysfunc(fileexist(&dir_car./Corp/csv/&meta._&st_RptYMD..csv)) %then %do;
		x "cd &dir_car./Corp/csv";
		x "file -bi &meta._&st_RptYMD..csv > chkencode.txt";
		data encode_info;
			infile "&dir_car./Corp/csv/chkencode.txt" 
			DLM='7F'x MISSOVER DSD ;
			INPUT  text   : $CHAR100.;
			encode = reverse(substr(reverse(strip(text)),1,5));
			if encode ="utf-8" then do;
				call symput("encode","");
			end;
			else do;
				call symput("encode","ywin");	
			end;
		run;
		%import_InputMaster(sheet=MetaInfo_&meta.,	out=raw_car.meta_&meta._&st_RptMth.);
		%gen_SRC_IN_CHAR(orig_src=&dir_car./Corp/csv/&meta._&st_RptYMD..csv, orig_delim=",", sas_tblname=stg_car.&meta._&st_RptMth., realtime_meta=work.rtmeta_&meta._&st_RptMth., encode=&encode.); 
		%trim_SRC_FIELDNAME(proposed_meta=raw_car.meta_&meta._&st_RptMth., realtime_meta=work.rtmeta_&meta._&st_RptMth., final_meta=stg_car.rtmeta_&meta._&st_RptMth.);
		%update_META2SRC(metatbl=stg_car.rtmeta_&meta._&st_RptMth., srctbl=stg_car.&meta._&st_RptMth., tartbl=raw_car.&meta._&st_RptMth.);
	%end;
	%else %do;
		%put ---- NCB LOG ---- : !! &dir_car./Corp/csv/&meta._&st_RptYMD..csv does not exist.;
	%end;
%end;
%mend;

%import_Format(src=CAR);
%exportlog(ind=START, path=&dir_log., name=CAR_CORPEXP_&st_RptMth._LOAD_&st_cltinfo.);
%gen_CAR(meta=CAR_CorpExp);
%exportlog(ind=STOP); 
%exportlog(ind=START, path=&dir_log., name=CAR_BANKFIEXP_&st_RptMth._LOAD_&st_cltinfo.);
%gen_CAR(meta=CAR_BankFIExp);
%exportlog(ind=STOP); 
%exportlog(ind=START, path=&dir_log., name=CAR_EXRATING_&st_RptMth._LOAD_&st_cltinfo.);
%gen_CAR(meta=CAR_ExRating);
%exportlog(ind=STOP); 

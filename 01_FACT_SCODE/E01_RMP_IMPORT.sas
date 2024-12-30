/* ************************************************************************************************ */
/* To import the source file in SAS dataset in characters format for avoiding information loss.		*/
/* The SAS dataset in characters format is saved in libname "STG", we will have regular				*/
/* housekeeping on every 2 years in this libname "STG".												*/
/* ************************************************************************************************ */

%macro gen_RMPData(t_rptmth=, txtfile=, meta=); 
%if %SYSFUNC(exist(raw_rmp.&txtfile._&t_rptmth.)) = 1 %then %do;
	%put ---- NCB LOG ---- : !! raw_rmp.&txtfile._&t_rptmth. is already existed.;
%end;
%else %do;
	%put ---- NCB LOG ---- : Start generating &txtfile. for the month of &t_rptmth.;
	%if %sysfunc(fileexist(&dir_rmp./&t_rptmth./&txtfile._043.txt)) %then %do;
		x "cd &dir_rmp./&t_rptmth.";
		x "file -bi &txtfile._043.txt > chkencode.txt";
		data encode_info;
			infile "&dir_rmp./&t_rptmth./chkencode.txt" 
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
		x "rm &dir_rmp./&t_rptmth./chkencode.txt";
		/*	
		%let t_rptmth=201504;
		%let txtfile=RMP2BIO5B;
		%let pmeta=raw_rmp.meta_rmp_reimsb_initial;
		%let meta=RMP_LCM;
		*/
		%let pmeta=raw_rmp.meta_&meta._&t_rptmth.;
		%import_InputMaster(sheet=MetaInfo_&meta., out=&pmeta.);
		%gen_SRC_IN_CHAR(orig_src=&dir_rmp./&t_rptmth./&txtfile._043.txt, orig_delim="|", sas_tblname=stg_rmp.&txtfile._&t_rptmth., realtime_meta=work.rtmeta_&txtfile._&t_rptmth., encode=&encode.); 
		%trim_SRC_FIELDNAME(proposed_meta=&pmeta., realtime_meta=work.rtmeta_&txtfile._&t_rptmth., final_meta=stg_rmp.rtmeta_&txtfile._&t_rptmth., txtfile=&txtfile.);
		%update_META2SRC(metatbl=stg_rmp.rtmeta_&txtfile._&t_rptmth., srctbl=stg_rmp.&txtfile._&t_rptmth., tartbl=raw_rmp.&txtfile._&t_rptmth.);
		%put ---- NCB LOG ---- : Finished generating &txtfile. for the month of &t_rptmth.;
	%end;
	%else %do;
		%put ---- NCB LOG ---- : !! &dir_rmp./&t_rptmth./&txtfile._043.txt does not exist.;
	%end;
%end;
%mend;

%import_Format(src=RMP);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO2,			meta=rmp_lcm);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOLCM,		meta=rmp_lcm);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO2B,		meta=rmp_lcmb);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOLCMB,		meta=rmp_lcmb);

%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO3,			meta=rmp_mm);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOMM,		meta=rmp_mm);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO3B,		meta=rmp_mmb);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOMMB,		meta=rmp_mmb);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO5A,		meta=rmp_reimsa);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOREIMSA,	meta=rmp_reimsa);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO5B,		meta=rmp_reimsb);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOREIMSB,	meta=rmp_reimsb);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO5C,		meta=rmp_reimsc);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOREIMSC,	meta=rmp_reimsc);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO7,			meta=rmp_redi);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOREDI,		meta=rmp_redi);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO7B,		meta=rmp_redib);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOREDIB,		meta=rmp_redib);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO10A,		meta=rmp_ofa);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOOFA,		meta=rmp_ofa);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO10B,		meta=rmp_ofb);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOOFB,		meta=rmp_ofb);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO10C,		meta=rmp_ofc);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOOFC,		meta=rmp_ofc);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO9A,		meta=rmp_pfa);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOPFA,		meta=rmp_pfa);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO9B,		meta=rmp_pfb);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOPFB,		meta=rmp_pfb);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO9C,		meta=rmp_pfc);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOPFC,		meta=rmp_pfc);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO12A,		meta=rmp_nbfiinsa);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIONBFIINSA,	meta=rmp_nbfiinsa);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO12B,		meta=rmp_nbfiinsb);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIONBFIINSB,	meta=rmp_nbfiinsb);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO13A,		meta=rmp_nbfiseca);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIONBFISECA,	meta=rmp_nbfiseca);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO13B,		meta=rmp_nbfisecb);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIONBFISECB,	meta=rmp_nbfisecb);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO11A,		meta=rmp_banka);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOBANKA,		meta=rmp_banka);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO11B,		meta=rmp_bankb);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOBANKB,		meta=rmp_bankb);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO11C,		meta=rmp_bankc);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOBANKC,		meta=rmp_bankc);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO11D,		meta=rmp_bankd);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOBANKD,		meta=rmp_bankd);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO11E,		meta=rmp_banke);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOBANKE,		meta=rmp_banke);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIO11F,		meta=rmp_bankf);
%gen_RMPData(t_rptmth=&st_rptmth.,txtfile=RMP2BIOBANKF,		meta=rmp_bankf);

// -----------------------------------------------------------
// Author:         Xin Shi <Xin.Shi@cern.ch>
//                 Stefano Mersi <Stefano.Mersi@cern.ch>
// Created:        Thr May 30 12:12 CET 2012
// Last modified:  Wed Jun 06 17:27 CET 2012
// -----------------------------------------------------------

#include <rootweb.hh>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>

#include <TTree.h>
#include <TH1F.h>
#include <TCanvas.h>
#include <boost/filesystem/operations.hpp>
#include <TFile.h>
#include <TH1D.h>
#include <TH2D.h>
#include <TStyle.h>

#define TARGETDIRECTORY "TARGETDIRECTORY"

using namespace std;
using namespace boost::filesystem;

int main(int argc, char** argv) {
  // string data_dir;
  void setTDRStyle(); 
  RootWContent* procCluster(string base_name, int id, TFile *f, bool verbose=false); 
  RootWContent* procConvert(string base_name, int id, TFile *f, bool verbose=false); 
  RootWContent* getTriggerPhase(string h_name_str, TFile *f, bool verbose=false);

  RootWContent* procOnTrackCluster(string base_name, int id, TFile *f, bool verbose=false); 
  RootWContent* procTracking(string base_name, int id, TFile *f, bool verbose=false); 
  RootWContent* procEfficiency(string base_name, TFile *f, bool verbose=false); 
  string get_id_str(int id); 
  char* get_dir_name(string base_name, string id_str="", string suffix=""); 

  string board_name, run_number;
  if (argc > 2){
    board_name = argv[1];
    run_number = argv[2];
  }
  else {
    // cout << "usage: " << argv[0] << " data/000003" << endl;    
    cout << "usage: " << argv[0] << " PixelTestBoard1 030623 [jobtype]" << endl;    
  }
  
  //size_t idx = data_dir.find("/");
  // string data_type = data_dir.substr(0, idx);
  // string run_number = data_dir.substr(idx+1);

  // string run_number = data_dir.substr(0, idx); 
  // string board_name = data_dir.substr(idx+1);

  
  // ---------------------------------------------------------
  // Get the root files 
  // ---------------------------------------------------------
  
  // TString convert_file = board_name + "/" + run_number + "-convert.root";
  // TString clusters_file = board_name + "/" + run_number + "-clustering.root";
  TString convert_file = run_number + "-convert.root";
  TString clusters_file = run_number + "-clustering.root";

  // cout << "cluster file: " << clusters_file << endl; 
  // TString tracks_file = data_dir + "/" + run_number + "-tracks.root";
  // TString tracks_noalign_file = board_name + "/" + run_number + "-tracks_noalign.root";
  TString tracks_noalign_file = run_number + "-tracks_prealign.root";

  
  TFile *f = new TFile(clusters_file);  
  
  // TFile *f2 = new TFile(tracks_file);
  TFile *f2;
  bool f2_exists = boost::filesystem::exists( tracks_noalign_file.Data() ); 
  if ( f2_exists ) {
       f2 = TFile::Open(tracks_noalign_file);
  }

//   if (f2->IsZombie()) {
//     // cout << "Error opening file: " << tracks_file << endl;
//     // cout << "Using " << tracks_noalign_file << " instead. " << endl;

//     f2->Close();

//     TFile *f2_alt = new TFile(tracks_noalign_file);
//     if (f2_alt->IsZombie()) {  
//       cout << "Error opening file: " << tracks_noalign_file << endl;
//     }

//     f2 = f2_alt;
//     //f2_alt->Close();

//   }

  // ---------------------------------------------------------
  // General Styles 
  // ---------------------------------------------------------
  setTDRStyle(); 
  
  // ---------------------------------------------------------
  // Plot settings for 2D 
  // ---------------------------------------------------------
  
  const UInt_t numberOfSteps = 5;
  const Int_t temperature_levels = 256; 

  gStyle->SetNumberContours(temperature_levels);
  
  Double_t stops[numberOfSteps] = { 0.00, 0.34, 0.61, 0.84, 1.00 };
  Double_t red[numberOfSteps]   = { 0.00, 0.00, 0.87, 1.00, 0.51 };
  Double_t green[numberOfSteps] = { 0.00, 0.81, 1.00, 0.20, 0.00 };
  Double_t blue[numberOfSteps]  = { 0.51, 1.00, 0.12, 0.00, 0.00 };
  Int_t myPalette[temperature_levels];
  
  Int_t colorIndex = TColor::CreateGradientColorTable(numberOfSteps,
			 stops, red, green, blue, temperature_levels);
  for (int i=0;i<temperature_levels;i++) myPalette[i] = colorIndex+i;
  gStyle->SetPalette(temperature_levels, myPalette);
  

  // ---------------------------------------------------------
  // Making website
  // ---------------------------------------------------------

  RootWSite* mySite = new RootWSite();
  mySite->setTitle("Test Beam DQM");
  mySite->setComment(run_number+string("/")+board_name);
  mySite->setCommentLink("../");
  mySite->addAuthor("Stefano Mersi");
  mySite->addAuthor("Xin Shi");
#ifdef REVISIONNUMBER
  mySite->setRevision(REVISIONNUMBER);
#endif
    



  // ---------------------------------------------------------
  // 1. Making Clusters w/o tracks page 
  // ---------------------------------------------------------

  string page_name = "clusters"; 
  RootWPage* myPage = new RootWPage(page_name); 
  myPage->setAddress("index.html");

  RootWContent* myContent; 

  cout << "Processing: " << page_name << " ... \n" << flush;        

  //TFile *f = new TFile(clusters_file);
  string base_name = "MyCMSPixelClusteringProcessor/detector_"; 
  char* dir_name;
  string id_str; 
  const int max_number_of_detectors = 20; 
  
  for (int n=0; n<max_number_of_detectors; n++) {
      id_str = get_id_str(n); 
      dir_name = get_dir_name(base_name, id_str);

      TDirectory *dir = f->GetDirectory(dir_name);
      if (dir) {
	myContent = procCluster(base_name, n, f);
	myPage->addContent(myContent);
      }
    } 
	
  // f->Close();

  // ---------------------------------------------------------
  // 1A. Making DCOL from the convert file
  // ---------------------------------------------------------
  RootWPage* myPage_convert = NULL; 
  
  TFile *f_convert;
  bool f_convert_exists = boost::filesystem::exists( convert_file.Data() ); 
  if ( f_convert_exists ) {
       f_convert = TFile::Open(convert_file);

  string page_name_convert = "convert"; 
  myPage_convert = new RootWPage(page_name_convert); 
  myPage_convert->setAddress("convert.html");

  // RootWContent* myContent; 

  cout << "Processing: " << page_name_convert << " ... \n" << flush;        

  // Get the triggerPhase plot

  base_name = "MyEUTelConvertCMSPixel/triggerPhase"; 
  myContent = getTriggerPhase(base_name, f_convert);
  myPage_convert->addContent(myContent); 

  //TFile *f = new TFile(clusters_file);
  base_name = "MyEUTelConvertCMSPixel/detector_"; 
  // char* dir_name;
  // string id_str; 
  // const int max_number_of_detectors = 20; 
  
  for (int n=0; n<max_number_of_detectors; n++) {
      id_str = get_id_str(n); 
      dir_name = get_dir_name(base_name, id_str);

      TDirectory *dir = f_convert->GetDirectory(dir_name);
      if (dir) {
	myContent = procConvert(base_name, n, f_convert);
	myPage_convert->addContent(myContent);
      }
    } 
	
  }


  // ---------------------------------------------------------
  // 2. Making on-tracks clusters page  
  // ---------------------------------------------------------
  RootWPage* myPage2 = NULL; 

  if (f2_exists) {
    string page_name2 = "on track cluster"; 
    // RootWPage* myPage2 = new RootWPage(page_name2); 
    myPage2 = new RootWPage(page_name2); 

    myPage2->setAddress("two.html");
    
    cout << "Processing: " << page_name2 << " ... \n" << flush;        
    
    //TFile *f2 = new TFile(tracks_file);
    base_name = "MyEUTelTestFitter"; 
    
    for (int n=0; n<max_number_of_detectors; n++) {
      myContent = procOnTrackCluster(base_name, n, f2);
      if (myContent != 0) {
	myPage2->addContent(myContent);
      }
    }
  }

  // ---------------------------------------------------------
  // 3. Making tracking page  
  // ---------------------------------------------------------
  RootWPage* myPage3= NULL ; 

  if (f2_exists) {
  string page_name3 = "tracking"; 
  myPage3 = new RootWPage(page_name3); 
  myPage3->setAddress("three.html");

  cout << "Processing: " << page_name3 << " ... \n" << flush;        
  
  // TFile *f3 = new TFile(tracks_file);
  base_name = "MyEUTelTestFitter"; 

  for (int n=0; n<max_number_of_detectors; n++) {
      myContent = procTracking(base_name, n, f2);
      if (myContent != 0) {
	myPage3->addContent(myContent);
      }
  }
  }

  // ---------------------------------------------------------
  // 4. Making Detectioin Efficiency page  
  // ---------------------------------------------------------
  RootWPage* myPage4 = NULL; 
  base_name = "MyEUTelFitTuple"; 

  if (f2_exists) {
    myContent = procEfficiency(base_name, f2);
    if (myContent != NULL) {
      
      string page_name4 = "efficiency"; 
      myPage4 = new RootWPage(page_name4); 
      myPage4->setAddress("four.html");
      
      cout << "Processing: " << page_name4 << " ... \n" << flush;        
      
      // TFile *f4 = new TFile(tracks_file);
      
      myPage4->addContent(myContent);
    }
  }

  // ---------------------------------------------------------
  // 5. Making Check Data Integrity page  
  // ---------------------------------------------------------
  // string datafile = string(board_name) + string("/") + string(run_number) + string("chk_dat.txt");
  string datafile = string(run_number) + string("chk_dat.txt");


  RootWPage* myPage5 = NULL ; 
  bool f5 = boost::filesystem::exists( datafile ); 
  if (f5) {
      string page_name5 = "data integrity"; 
      myPage5 = new RootWPage(page_name5); 
      myPage5->setAddress("data_integrity.html");
      
      cout << "Processing: " << page_name5 << " ... \n" << flush;        
      RootWContent* myContent5 = new RootWContent("Data Integrity Check");
      
      ifstream infile;
      infile.open(datafile.c_str()); 
      
      std::string s;
      int nline = 0; 
      while (std::getline(infile, s))
	{
	  nline ++; 
	  myContent5->addParagraph(s);
	  
	  if (nline > 100) {
	    myContent5->addParagraph("\n\nExceeding 100 lines limit!!!");
	    break; 
	  }
	}
      
      myPage5->addContent(myContent5 );
    }

  // make the full data_check file link 
  
  
  // string datafile_ful = string(board_name) + string("/") + string(run_number) + 
  //   string("check_data_integrity.txt");
  string datafile_ful = string(run_number) + string("check_data_integrity.txt");
  
  if ( boost::filesystem::exists( datafile_ful ) ) { 
    RootWContent* myContent5_ful = new RootWContent("Full Data Integrity Check");
    string destinationFilename = "check_data_integrity.txt"; 
    
    ifstream infile;
    infile.open(datafile_ful.c_str()); 
      
    std::string s;
    int nline = 0; 
    while (std::getline(infile, s))
      {
	nline ++; 
	myContent5_ful->addParagraph(s);
	
	if (nline > 100) {
	  myContent5_ful->addParagraph("\n\nExceeding 100 lines limit!!!");
	  break; 
	}
      }
 
    RootWBinaryFile* myBinaryFile = new RootWBinaryFile(destinationFilename, "The original file link: ", datafile_ful);
    myContent5_ful->addItem(myBinaryFile);

    myPage5->addContent(myContent5_ful); 
  }

  // ---------------------------------------------------------
  // Adding all pages
  // ---------------------------------------------------------

  mySite->addPage(myPage);
 
  if ( myPage_convert != NULL)
    mySite->addPage(myPage_convert);
  if ( myPage2 != NULL)   
    mySite->addPage(myPage2);
  if ( myPage3 != NULL)
    mySite->addPage(myPage3);
  if (myPage4 != NULL) 
    mySite->addPage(myPage4);
  if (myPage5 != NULL)
    mySite->addPage(myPage5);


  std::string targetDirectory = string(getenv(TARGETDIRECTORY));
  if (targetDirectory=="") {
    cerr << "Warning: cannot find the definition of target directory "
         << "by the environment variable " << TARGETDIRECTORY << endl
         << "Using current directory as target directory" << endl;
    targetDirectory=".";
  }
 
  if (!boost::filesystem::exists( targetDirectory )) {
    if (!boost::filesystem::create_directory(targetDirectory)) {
      cerr << "Couldn't create directory " << targetDirectory << endl;
      return 0;
    }
  }

  // targetDirectory += "/" + data_type + "_" + run_number;
  targetDirectory += "/" + run_number + "_" + board_name;

  mySite->setTargetDirectory(targetDirectory);

  cout << "Making pages: " << endl;

  mySite->makeSite(true);
       
  f->Close();
  
  if (f2_exists)
    f2->Close();

  // if (f2_alt) f2_alt->Close();

  cout << "Saved at: " << targetDirectory << endl;

  return 0;
}


RootWContent* procCluster(string base_name, int id, TFile *f, bool verbose=false) {

  string id_str; 
  stringstream convert; 
  char* get_hname(string a, string b, string id_str, string suffix=""); 
  
  int ww = 850; 
  int wh = 600; 
  
  convert << id;
  id_str = convert.str(); 

  //   RootWContent* myContent = new RootWContent("Detector 0");
  string content_name = "Detector " + id_str;  

  if (verbose) cout << content_name << " ..." << flush;
  RootWContent* myContent = new RootWContent(content_name); 
  
  TCanvas* myCanvas = new TCanvas(); 
  
  //  string base_name = "MyCMSPixelClusteringProcessor/detector_"; 
  
  char* h_name; 
  // ---------------------------------------------------------
  // 1.1 Number of pixels per event 
  // ---------------------------------------------------------
  //  TH1D *pixelPerEvent_d0 = (TH1D*)f->Get("MyCMSPixelClusteringProcessor/detector_0/pixelPerEvent_d0");
  h_name = get_hname(base_name, "/pixelPerEvent_d", id_str);


  TH1D *pixelPerEvent_d0 = (TH1D*)f->Get(h_name);

  myCanvas->cd();
  pixelPerEvent_d0->Draw(); 
  RootWImage* pixelPerEvent_d0_img = new RootWImage(myCanvas, ww, wh);
  myContent->addItem(pixelPerEvent_d0_img);

  // ---------------------------------------------------------
  // 1.2 Total charge per pixel - 2D 
  // ---------------------------------------------------------
  //  TH2D *chargeMap_d0 = (TH2D*)f->Get("MyCMSPixelClusteringProcessor/detector_0/chargeMap_d0");

  h_name = get_hname(base_name, "/chargeMap_d", id_str);
  TH2D *chargeMap_d0 = (TH2D*)f->Get(h_name); 

  chargeMap_d0->GetYaxis()->SetTitle("Pixel ID in Y");
  chargeMap_d0->GetXaxis()->SetTitle("Pixel ID in X");

  chargeMap_d0->GetZaxis()->SetLabelSize(0.02);

  myCanvas->cd();

  chargeMap_d0->Draw("colz");
  RootWImage*  chargeMap_d0_img = new RootWImage(myCanvas, ww, wh); 
  myContent->addItem(chargeMap_d0_img);
  

  // ---------------------------------------------------------
  // 1.3 Total charge in ADC / Cluster energy 1D
  // ---------------------------------------------------------
  //TH1D *clusterSignal_d0 = (TH1D*)f->Get("MyCMSPixelClusteringProcessor/detector_0/clusterSignal_d0");
  h_name = get_hname(base_name, "/clusterSignal_d", id_str);
  TH1D *clusterSignal_d0 = (TH1D*)f->Get(h_name);
  

  myCanvas->cd();
  clusterSignal_d0->Draw();
  RootWImage*  clusterSignal_d0_img = new RootWImage(myCanvas, ww, wh); 
  myContent->addItem(clusterSignal_d0_img);

  // ---------------------------------------------------------
  // 1.4 Position of clusters 2D 
  // ---------------------------------------------------------

  //  TH2D *hitMap_d0 = (TH2D*)f->Get("MyCMSPixelClusteringProcessor/detector_0/hitMap_d0");
  h_name = get_hname(base_name, "/hitMap_d", id_str);
  TH2D *hitMap_d0 = (TH2D*)f->Get(h_name);

  hitMap_d0->GetYaxis()->SetTitle("Pixel ID in Y");
  hitMap_d0->GetXaxis()->SetTitle("Pixel ID in X");
  hitMap_d0->GetZaxis()->SetLabelSize(0.02);

  myCanvas->cd();
  hitMap_d0->Draw("colz");
  RootWImage* hitMap_d0_img = new RootWImage(myCanvas, ww, wh); 
  myContent->addItem(hitMap_d0_img);


  // ---------------------------------------------------------
  // 1.5 Cluster size 
  // ---------------------------------------------------------
  //  TH1D *clustersize_d0 = (TH1D*)f->Get("MyCMSPixelClusteringProcessor/detector_0/clustersize_d0");

  h_name = get_hname(base_name, "/clustersize_d", id_str);
  TH1D *clustersize_d0 =  (TH1D*)f->Get(h_name);

  myCanvas->cd();
  clustersize_d0->Draw();
  RootWImage*  clustersize_d0_img = new RootWImage(myCanvas, ww, wh);
  myContent->addItem(clustersize_d0_img);

  // ---------------------------------------------------------
  // 1.6 Cluster shape X 
  // ---------------------------------------------------------
  //  TH1D *Xclusterwidth_d0 = (TH1D*)f->Get("MyCMSPixelClusteringProcessor/detector_0/Xclusterwidth_d0");
  h_name = get_hname(base_name, "/Xclusterwidth_d", id_str);
  TH1D *Xclusterwidth_d0 = (TH1D*)f->Get(h_name);

  myCanvas->cd();
  Xclusterwidth_d0->Draw();
  RootWImage*  Xclusterwidth_d0_img = new RootWImage(myCanvas, ww, wh); 
  myContent->addItem(Xclusterwidth_d0_img);

  // ---------------------------------------------------------
  // 1.7 Cluster shape Y 
  // ---------------------------------------------------------
  //  TH1D *Yclusterwidth_d0 = (TH1D*)f->Get("MyCMSPixelClusteringProcessor/detector_0/Yclusterwidth_d0");

  h_name = get_hname(base_name, "/Yclusterwidth_d", id_str);
  TH1D *Yclusterwidth_d0 = (TH1D*)f->Get(h_name);

  myCanvas->cd();
  Yclusterwidth_d0->Draw();
  RootWImage*  Yclusterwidth_d0_img = new RootWImage(myCanvas, ww, wh); 
  myContent->addItem(Yclusterwidth_d0_img);


  // ---------------------------------------------------------
  //  1.8 Cluster size vs. charge 
  // ---------------------------------------------------------
  //TH2D *clusterSizeVsCharge_d0 = (TH2D*)f->Get("MyCMSPixelClusteringProcessor/detector_0/clusterSizeVsCharge_d0");
  h_name = get_hname(base_name, "/clusterSizeVsCharge_d", id_str);
  TH2D *clusterSizeVsCharge_d0 = (TH2D*)f->Get(h_name); 
  
  clusterSizeVsCharge_d0->GetYaxis()->SetTitle("Charge");
  clusterSizeVsCharge_d0->GetXaxis()->SetTitle("Cluster size");

  clusterSizeVsCharge_d0->GetZaxis()->SetLabelSize(0.02);

  myCanvas->cd();
  clusterSizeVsCharge_d0->Draw("colz");
  RootWImage*  clusterSizeVsCharge_d0_img = new RootWImage(myCanvas, ww, wh);
  myContent->addItem(clusterSizeVsCharge_d0_img);


// ---------------------------------------------------------
//  1.9 Column Hits vs Event 
// ---------------------------------------------------------
  
  /*
  h_name = get_hname(base_name, "/colTime_d", id_str);
  TH2D *colTime = (TH2D*)f->Get(h_name); 
  
  if (colTime) {
    colTime->GetYaxis()->SetTitle("Col");
    colTime->GetXaxis()->SetTitle("Events");
    
    colTime->GetZaxis()->SetLabelSize(0.02);
    
    myCanvas->cd();
    colTime->Draw("colz");
    RootWImage* colTime_img = new RootWImage(myCanvas, ww, wh); 
    myContent->addItem(colTime_img);

  }
  */ 

  TH2D *colTime; 
      
  for (int i=0; i<4; i++){

    TString hname = Form("/colTime%d_d", i); 
    // cout << "i=" << i << ", hname = " <<  hname << endl; 
    h_name = get_hname(base_name, hname.Data(), id_str);
    colTime = (TH2D*)f->Get(h_name);

    if (colTime) {
      colTime->GetYaxis()->SetTitle("Col");
      colTime->GetXaxis()->SetTitle("Events");
      colTime->GetZaxis()->SetLabelSize(0.02);
      
      myCanvas->cd();
      colTime->Draw("colz");
      RootWImage* colTime_img = new RootWImage(myCanvas, ww, wh); 
      myContent->addItem(colTime_img);
    }
  }
    
  if (verbose) cout << " OK." << endl; 
  return myContent; 
}


RootWContent* procConvert(string base_name, int id, TFile *f, bool verbose=false) {

  string id_str; 
  stringstream convert; 
  char* get_hname(string a, string b, string id_str, string suffix=""); 
  
  int ww = 850; 
  int wh = 600; 
  
  convert << id;
  id_str = convert.str(); 

  //   RootWContent* myContent = new RootWContent("Detector 0");
  string content_name = "detector " + id_str;  

  if (verbose) cout << content_name << " ..." << flush;
  RootWContent* myContent = new RootWContent(content_name); 
  
  TCanvas* myCanvas = new TCanvas(); 
  
  char* h_name; 
  // ---------------------------------------------------------
  // DCOL plot 
  // ---------------------------------------------------------
  h_name = get_hname(base_name, "/dcolMonitorEvt_d", id_str);

  TH2D *colTime; 
      
  colTime = (TH2D*)f->Get(h_name);

  if (colTime) {
    colTime->GetYaxis()->SetTitle("Col");
    colTime->GetXaxis()->SetTitle("Events");
    colTime->GetZaxis()->SetLabelSize(0.02);
      
    myCanvas->cd();
    colTime->Draw("colz");
    RootWImage* colTime_img = new RootWImage(myCanvas, ww, wh); 
    myContent->addItem(colTime_img);

    if (verbose) cout << " OK." << endl; 
  }
  return myContent; 
}


RootWContent* getTriggerPhase(string h_name_str, TFile *f, bool verbose=false) {
  stringstream convert; 
  string content_name = "TriggerPhase "; 
    
  int ww = 850; 
  int wh = 600; 
  
  if (verbose) cout << content_name << " ..." << flush;
  RootWContent* myContent = new RootWContent(content_name); 
  
  TCanvas* myCanvas = new TCanvas(); 
  
  // char* h_name;
  char *h_name = new char[h_name_str.size()+1]; 
  h_name[h_name_str.size()]=0;
  memcpy(h_name,h_name_str.c_str(),h_name_str.size());
  
  // ---------------------------------------------------------
  // Trigger Phase plot 
  // ---------------------------------------------------------
  // h_name = get_hname(base_name, "/dcolMonitorEvt_d", id_str);

  TH1D *colTime; 
      
  colTime = (TH1D*)f->Get(h_name);

  if (colTime) {
    // colTime->GetYaxis()->SetTitle("Col");
    // colTime->GetXaxis()->SetTitle("Events");
    // colTime->GetZaxis()->SetLabelSize(0.02);
      
    myCanvas->cd();
    colTime->Draw(); // "colz");
    RootWImage* colTime_img = new RootWImage(myCanvas, ww, wh); 
    myContent->addItem(colTime_img);

    if (verbose) cout << " OK." << endl; 
  }
  return myContent; 
}

void setTDRStyle() {

  TStyle *tdrStyle = new TStyle("tdrStyle","Style for P-TDR");

// For the canvas:
  tdrStyle->SetCanvasBorderMode(0);
  tdrStyle->SetCanvasColor(kWhite);
  tdrStyle->SetCanvasDefH(600); //Height of canvas
  tdrStyle->SetCanvasDefW(600); //Width of canvas
  tdrStyle->SetCanvasDefX(0);   //POsition on screen
  tdrStyle->SetCanvasDefY(0);

// For the Pad:
  tdrStyle->SetPadBorderMode(0);
  // tdrStyle->SetPadBorderSize(Width_t size = 1);
  tdrStyle->SetPadColor(kWhite);
  tdrStyle->SetPadGridX(false);
  tdrStyle->SetPadGridY(false);
  tdrStyle->SetGridColor(0);
  tdrStyle->SetGridStyle(3);
  tdrStyle->SetGridWidth(1);

// For the frame:
  tdrStyle->SetFrameBorderMode(0);
  tdrStyle->SetFrameBorderSize(1);
  tdrStyle->SetFrameFillColor(0);
  tdrStyle->SetFrameFillStyle(0);
  tdrStyle->SetFrameLineColor(1);
  tdrStyle->SetFrameLineStyle(1);
  tdrStyle->SetFrameLineWidth(1);

// For the histo:
  // tdrStyle->SetHistFillColor(1);
  // tdrStyle->SetHistFillStyle(0);
  tdrStyle->SetHistLineColor(1);
  tdrStyle->SetHistLineStyle(0);
  tdrStyle->SetHistLineWidth(1);
  // tdrStyle->SetLegoInnerR(Float_t rad = 0.5);
  // tdrStyle->SetNumberContours(Int_t number = 20);

  tdrStyle->SetEndErrorSize(2);
  // tdrStyle->SetErrorMarker(20);
  tdrStyle->SetMarkerStyle(20);
  tdrStyle->SetErrorX(0.);
  //tdrStyle->SetCenterTitle(); 

// For the fit/function:
  tdrStyle->SetOptFit(1);
  tdrStyle->SetFitFormat("5.4g");
  tdrStyle->SetFuncColor(2);
  tdrStyle->SetFuncStyle(1);
  tdrStyle->SetFuncWidth(1);


//For the date:
  tdrStyle->SetOptDate(0);
  // tdrStyle->SetDateX(Float_t x = 0.01);
  // tdrStyle->SetDateY(Float_t y = 0.01);


// For the statistics box:
  tdrStyle->SetOptFile(0);
  // tdrStyle->SetOptStat("mr"); // To display the mean and RMS:   SetOptStat("mr");

  tdrStyle->SetOptStat(1111111); // To display the mean and RMS:   SetOptStat("mr");
  tdrStyle->SetStatColor(kWhite);
  tdrStyle->SetStatFont(42);
  tdrStyle->SetStatFontSize(0.025);
  tdrStyle->SetStatTextColor(1);
  tdrStyle->SetStatFormat("6.4g");
  tdrStyle->SetStatBorderSize(1);
  tdrStyle->SetStatH(0.1);
  tdrStyle->SetStatW(0.15);
  // tdrStyle->SetStatStyle(Style_t style = 1001);
  // tdrStyle->SetStatX(Float_t x = 0);
  // tdrStyle->SetStatY(Float_t y = 0);

// Margins:
  tdrStyle->SetPadTopMargin(0.08);
  tdrStyle->SetPadBottomMargin(0.1);
  tdrStyle->SetPadLeftMargin(0.1);
  tdrStyle->SetPadRightMargin(0.3);

// For the Global title:
  // tdrStyle->SetOptTitle(0);
  tdrStyle->SetTitleFont(42);
  tdrStyle->SetTitleColor(1);
  tdrStyle->SetTitleTextColor(1);
  tdrStyle->SetTitleFillColor(10);
  tdrStyle->SetTitleFontSize(0.04);
  // tdrStyle->SetTitleH(0); // Set the height of the title box
  // tdrStyle->SetTitleW(0); // Set the width of the title box
  // tdrStyle->SetTitleX(0); // Set the position of the title box
  // tdrStyle->SetTitleY(0.985); // Set the position of the title box
  // tdrStyle->SetTitleStyle(Style_t style = 1001);
  tdrStyle->SetTitleBorderSize(0);
  
  // tdrStyle->SetTitleAlign(13);
  // tdrStyle->SetTitleX(.99); 

  // For the axis titles:
  tdrStyle->SetTitleColor(1, "XYZ");
  tdrStyle->SetTitleFont(42, "XYZ");
  tdrStyle->SetTitleSize(0.01, "XYZ");
  // tdrStyle->SetTitleXSize(Float_t size = 0.02); // Another way to set the size?
  // tdrStyle->SetTitleYSize(Float_t size = 0.02);

  //  tdrStyle->SetTitleXOffset(0.9);
  // tdrStyle->SetTitleYOffset(1.05);

  // tdrStyle->SetTitleOffset(1.1, "Y"); // Another way to set the Offset


// For the axis labels:
  tdrStyle->SetLabelColor(1, "XYZ");
  tdrStyle->SetLabelFont(42, "XYZ");
  //  tdrStyle->SetLabelOffset(0.007, "XYZ");
  tdrStyle->SetLabelSize(0.02, "XYZ");

// For the axis:
  tdrStyle->SetAxisColor(1, "XYZ");
  tdrStyle->SetStripDecimals(kTRUE);
  tdrStyle->SetTickLength(0.03, "XYZ");
  tdrStyle->SetNdivisions(510, "XYZ");
  tdrStyle->SetPadTickX(1);  // To get tick marks on the opposite side of the frame
  tdrStyle->SetPadTickY(1);

// Change for log plots:
  tdrStyle->SetOptLogx(0);
  tdrStyle->SetOptLogy(0);
  tdrStyle->SetOptLogz(0);



// Postscript options:
  // tdrStyle->SetPaperSize(15.,15.);
  // tdrStyle->SetLineScalePS(Float_t scale = 3);
  // tdrStyle->SetLineStyleString(Int_t i, const char* text);
  // tdrStyle->SetHeaderPS(const char* header);
  // tdrStyle->SetTitlePS(const char* pstitle);

  // tdrStyle->SetBarOffset(Float_t baroff = 0.5);
  // tdrStyle->SetBarWidth(Float_t barwidth = 0.5);
  // tdrStyle->SetPaintTextFormat(const char* format = "g");
  // tdrStyle->SetPalette(Int_t ncolors = 0, Int_t* colors = 0);
  // tdrStyle->SetTimeOffset(Double_t toffset);
  // tdrStyle->SetHistMinimumZero(kTRUE);

  tdrStyle->cd();

}




string get_id_str(int id){
  string id_str; 
  stringstream convert; 
  convert << id;
  id_str = convert.str(); 
  return id_str;
}


char* get_dir_name(string base_name, string id_str="", string suffix=""){
  string s = base_name + id_str + suffix;
  char *h_name = new char[s.size()+1]; 
  h_name[s.size()]=0;
  memcpy(h_name,s.c_str(),s.size());
  return h_name;
}


char* get_hname(string a, string b, string id_str, string suffix=""){
  string h_name_str = a + id_str + b + id_str + suffix; 
  char *h_name = new char[h_name_str.size()+1]; 
  h_name[h_name_str.size()]=0;
  memcpy(h_name,h_name_str.c_str(),h_name_str.size());
  return h_name;
}

char* get_hname_tracks(string a, string b, string id_str, string suffix=""){
  string h_name_str = a + b + id_str + suffix; 
  char *h_name = new char[h_name_str.size()+1]; 
  h_name[h_name_str.size()]=0;
  memcpy(h_name,h_name_str.c_str(),h_name_str.size());
  return h_name;
}


char* get_tname_tracks(string a, string b){
  string t_name_str = a + b; 
  char *t_name = new char[t_name_str.size()+1]; 
  t_name[t_name_str.size()]=0;
  memcpy(t_name,t_name_str.c_str(),t_name_str.size());
  return t_name;
}

RootWContent* procOnTrackCluster(string base_name, int id, TFile *f, bool verbose=false) {
  int ww = 850; 
  int wh = 600; 

  string id_str; 
  stringstream convert; 
  char* get_hname_tracks(string a, string b, string id_str, string suffix=""); 

  convert << id;
  id_str = convert.str(); 

  string content_name = "Detector " + id_str;  
  RootWContent* myContent = new RootWContent(content_name); 
  
  TCanvas* myCanvas = new TCanvas(); 
  
  char* h_name; 
  // ---------------------------------------------------------
  // 2.1 On Track Hits 
  // ---------------------------------------------------------
  h_name = get_hname_tracks(base_name, "/pl", id_str, "_onTrackHits");

  // cout << h_name << endl; 
  TH2D *pl0_onTrackHits = (TH2D*)f->Get(h_name);

  // cout << "TH2D: " << pl0_onTrackHits << endl; 
  if (!pl0_onTrackHits) return 0;
  
  if (verbose) cout << content_name << " ..." << flush; 
  myCanvas->cd();

  pl0_onTrackHits->Draw("colz");
  RootWImage*  pl0_onTrackHits_img = new RootWImage(myCanvas, ww, wh);
  myContent->addItem(pl0_onTrackHits_img);

  if (verbose) cout << " OK." << endl; 
  return myContent; 

}

RootWContent* procTracking(string base_name, int id, TFile *f, bool verbose=false) {

  string id_str; 
  stringstream convert; 
  char* get_hname_tracks(string a, string b, string id_str, string suffix=""); 

  convert << id;
  id_str = convert.str(); 

  int ww = 850; 
  int wh = 600; 

  string content_name = "Detector " + id_str;  
  RootWContent* myContent = new RootWContent(content_name); 
  
  TCanvas* myCanvas = new TCanvas(); 
  
  char* h_name; 
  // ---------------------------------------------------------
  // 3.1 Residuals X 
  // ---------------------------------------------------------
  h_name = get_hname_tracks(base_name, "/pl", id_str, "_residualX");

  TH1D *pl0_residualX = (TH1D*)f->Get(h_name);

  if (!pl0_residualX) return 0;  
  
  if (verbose) cout << content_name << " ..." << flush; 
  myCanvas->cd();


  pl0_residualX->GetXaxis()->SetTitle("X residual [mm]");

  pl0_residualX->Draw();
  RootWImage*  pl0_residualX_img = new RootWImage(myCanvas, ww, wh);
  myContent->addItem(pl0_residualX_img);


  // ---------------------------------------------------------
  // 3.2 Residuals Y
  // ---------------------------------------------------------
  h_name = get_hname_tracks(base_name, "/pl", id_str, "_residualY");

  TH1D *pl0_residualY = (TH1D*)f->Get(h_name);

  if (!pl0_residualY) return 0; 
  
  myCanvas->cd();

  pl0_residualY->GetXaxis()->SetTitle("Y residual [mm]");
  pl0_residualY->Draw();
  RootWImage*  pl0_residualY_img = new RootWImage(myCanvas, ww, wh);
  myContent->addItem(pl0_residualY_img);


  // ---------------------------------------------------------
  // 3.3 Pulls X 
  // ---------------------------------------------------------
  h_name = get_hname_tracks(base_name, "/pl", id_str, "_pullsX");

  TH1D *pl0_pullsX = (TH1D*)f->Get(h_name);

  if (!pl0_pullsX) return 0;  
  
  myCanvas->cd();
  pl0_pullsX->Draw();
  RootWImage*  pl0_pullsX_img = new RootWImage(myCanvas, ww, wh);
  myContent->addItem(pl0_pullsX_img);

  // ---------------------------------------------------------
  // 3.4 Pulls Y
  // ---------------------------------------------------------
  h_name = get_hname_tracks(base_name, "/pl", id_str, "_pullsY");

  TH1D *pl0_pullsY = (TH1D*)f->Get(h_name);

  if (!pl0_pullsY) return 0;  
  
  myCanvas->cd();
  pl0_pullsY->Draw();
  RootWImage*  pl0_pullsY_img = new RootWImage(myCanvas, ww, wh);
  myContent->addItem(pl0_pullsY_img);


  if (verbose) cout << " OK." << endl; 
  return myContent; 

}

RootWContent* procEfficiency(string base_name, TFile *f, bool verbose=false) {
  char* get_tname_tracks(string a, string b);
  string content_name = "Efficiency"; 
  RootWContent* myContent = new RootWContent(content_name); 
  
  TCanvas* myCanvas = new TCanvas(); 

  int ww = 850; 
  int wh = 600; 
  
  char* t_name; 
  // ---------------------------------------------------------
  // 4.1 Detection Eff 
  // ---------------------------------------------------------
  t_name = get_tname_tracks(base_name, "/EUFitEff");

  TTree *EUFitEff = (TTree*)f->Get(t_name);

  // if (!EUFitEff) return 0;

  if ( EUFitEff && EUFitEff->GetBranch("DetectionEff_DUTId4") ) {
    
    myCanvas->cd();
    EUFitEff->Draw("DetectionEff_DUTId4");
    RootWImage*  DetectionEff_DUTId4_img = new RootWImage(myCanvas, ww, wh);
    myContent->addItem(DetectionEff_DUTId4_img);
    return myContent; 
  }
  
  return NULL; 

}

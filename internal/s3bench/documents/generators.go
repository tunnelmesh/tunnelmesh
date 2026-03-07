package documents

import (
	"fmt"
	"strings"

	"github.com/tunnelmesh/tunnelmesh/internal/s3bench/story"
)

// GenerateDocument creates a document with a convincing title and lorem ipsum content.
// TODO: Replace lorem ipsum with Ollama-generated content for interesting prose.
func GenerateDocument(docType string, ctx story.Context) ([]byte, error) {
	docNum := ctx.Data["DocumentNumber"].(int)
	docID := GenerateDocID(docType, docNum, ctx.Timestamp)
	phase := GetPhase(ctx.StoryElapsed)

	var doc strings.Builder

	// Classification banner for clearance levels
	if ctx.Author.Clearance >= 3 {
		doc.WriteString(FormatClassification(ctx.Author.Clearance))
		doc.WriteString("\n\n")
	}

	// Document header with convincing title
	doc.WriteString(generateTitle(docType, phase, docNum))
	fmt.Fprintf(&doc, "\nDocument ID: %s\n", docID)
	fmt.Fprintf(&doc, "Timestamp: %s\n", FormatTimestamp(ctx.Timestamp))
	fmt.Fprintf(&doc, "Author: %s\n", ctx.Author.Name)
	fmt.Fprintf(&doc, "Phase: %s (H+%d)\n", PhaseName(phase), int(ctx.StoryElapsed.Hours()))
	doc.WriteString("\n")
	doc.WriteString(strings.Repeat("=", 60))
	doc.WriteString("\n\n")

	// Content (lorem ipsum for now, Ollama later)
	doc.WriteString(LoremIpsum)
	doc.WriteString("\n\n")
	doc.WriteString(LoremIpsum)

	// Footer
	if ctx.Author.Clearance >= 3 {
		doc.WriteString("\n\n")
		doc.WriteString(strings.Repeat("=", 60))
		doc.WriteString("\n")
		doc.WriteString(FormatClassification(ctx.Author.Clearance))
	}

	return []byte(doc.String()), nil
}

// generateTitle creates a convincing document title based on type and phase
func generateTitle(docType string, phase, docNum int) string {
	titles := map[string][]string{
		"battle_report": {
			"BATTLE REPORT: ENGAGEMENT SECTOR-%d",
			"TACTICAL ANALYSIS: OPERATION IRONCLAD-%d",
			"COMBAT ASSESSMENT: DENVER DEFENSE LINE %d",
		},
		"sitrep": {
			"SITUATION REPORT %04d - EMERGENCY STATUS",
			"SITREP-%04d: TACTICAL OVERVIEW",
			"STATUS UPDATE %04d - CRITICAL",
		},
		"scientific_analysis": {
			"SCIENTIFIC ANALYSIS %d: XENOBIOLOGY FINDINGS",
			"RESEARCH PAPER %d: ADVANCED PROPULSION SYSTEMS",
			"TECHNICAL BRIEF %d: ENERGY WEAPON ANALYSIS",
		},
		"traffic_report": {
			"TRAFFIC ANALYSIS %d: DENVER METROPOLITAN AREA",
			"MOVEMENT LOG %d: CIVILIAN EVACUATION STATUS",
			"TRANSIT REPORT %d: INFRASTRUCTURE ASSESSMENT",
		},
		"council_meeting": {
			"NATIONAL SECURITY COUNCIL MEETING #%d",
			"EMERGENCY SESSION %d: CRISIS RESPONSE PLANNING",
			"CLASSIFIED BRIEFING %d: STRATEGIC OPTIONS",
		},
		"telephone": {
			"PHONE TRANSCRIPT %04d: EMERGENCY COMMUNICATION",
			"SECURE CALL LOG %04d",
			"COMMUNICATION RECORD %04d",
		},
		"im_transcript": {
			"IM CHAT LOG %04d",
			"SECURE MESSAGING THREAD %04d",
			"COMMUNICATION TRANSCRIPT %04d",
		},
		"casualty_list": {
			"CASUALTY REPORT %d",
			"KIA/WIA/MIA LIST %d",
			"PERSONNEL LOSSES - UPDATE %d",
		},
		"supply_manifest": {
			"SUPPLY MANIFEST %d",
			"LOGISTICS INVENTORY %d",
			"RESOURCE STATUS REPORT %d",
		},
		"intel_brief": {
			"INTELLIGENCE BRIEF %d",
			"THREAT ASSESSMENT %d",
			"TACTICAL INTELLIGENCE UPDATE %d",
		},
		"radio_log": {
			"RADIO COMMUNICATIONS LOG %d",
			"TACTICAL RADIO TRANSCRIPT %d",
			"FIELD COMMUNICATIONS %d",
		},
		"press_release": {
			"OFFICIAL STATEMENT %d",
			"PUBLIC ANNOUNCEMENT %d",
			"PRESS BRIEFING %d",
		},
		"private_diary": {
			"Personal Journal Entry #%d",
			"Day %d - Personal Log",
			"Diary Entry %d",
		},
		"hospital_records": {
			"PATIENT FILE %04d",
			"MEDICAL RECORD %04d",
			"TREATMENT LOG %04d",
		},
		"lab_results": {
			"LABORATORY ANALYSIS %d",
			"TEST RESULTS %d",
			"DIAGNOSTIC REPORT %d",
		},
		"field_notes": {
			"FIELD OBSERVATION %d",
			"TACTICAL NOTES %d",
			"GROUND REPORT %d",
		},
		"drone_footage": {
			"UAV SURVEILLANCE LOG %d",
			"DRONE FOOTAGE SUMMARY %d",
			"AERIAL RECONNAISSANCE %d",
		},
		"intercepted_comms": {
			"INTERCEPTED TRANSMISSION %d",
			"SIGNALS INTELLIGENCE %d",
			"DECODED MESSAGE %d",
		},
		"evacuation_order": {
			"EVACUATION ORDER %d",
			"EMERGENCY DIRECTIVE %d",
			"MANDATORY EVACUATION - SECTOR %d",
		},
		"status_update": {
			"STATUS UPDATE %04d",
			"OPERATIONAL BRIEF %04d",
			"CURRENT SITUATION %04d",
		},
		"incident_report": {
			"INCIDENT REPORT %04d",
			"AFTER ACTION REPORT %d",
			"EVENT SUMMARY %d",
		},
		"sensor_reading": {
			"SENSOR DATA %04d",
			"ATMOSPHERIC ANALYSIS %d",
			"ENVIRONMENTAL SCAN %d",
		},
		"social_media": {
			"Social Media Post #%d",
			"Public Update %d",
			"Citizen Report %d",
		},
		"news_bulletin": {
			"NEWS BULLETIN %d",
			"BREAKING NEWS %d",
			"MEDIA ALERT %d",
		},
		"logistics_report": {
			"LOGISTICS REPORT %d",
			"SUPPLY CHAIN STATUS %d",
			"RESOURCE ALLOCATION %d",
		},
		"personnel_status": {
			"PERSONNEL STATUS %d",
			"UNIT READINESS REPORT %d",
			"MANPOWER ASSESSMENT %d",
		},
		"environmental_report": {
			"ENVIRONMENTAL REPORT %d",
			"WEATHER ANALYSIS %d",
			"ATMOSPHERIC CONDITIONS %d",
		},
		"email": {
			"Email Message %04d",
			"Correspondence %04d",
			"Internal Communication %04d",
		},
		"alert_message": {
			"ALERT %04d",
			"WARNING NOTIFICATION %04d",
			"EMERGENCY ALERT %04d",
		},
	}

	titleTemplates, ok := titles[docType]
	if !ok {
		return fmt.Sprintf("DOCUMENT %d: %s", docNum, strings.ToUpper(docType))
	}

	template := titleTemplates[docNum%len(titleTemplates)]
	return fmt.Sprintf(template, docNum)
}

// All the individual generator functions now just call GenerateDocument

func GenerateBattleReport(ctx story.Context) ([]byte, error) {
	return GenerateDocument("battle_report", ctx)
}

func GenerateSITREP(ctx story.Context) ([]byte, error) {
	return GenerateDocument("sitrep", ctx)
}

func GenerateScientificAnalysis(ctx story.Context) ([]byte, error) {
	return GenerateDocument("scientific_analysis", ctx)
}

func GenerateTrafficReport(ctx story.Context) ([]byte, error) {
	return GenerateDocument("traffic_report", ctx)
}

func GenerateCouncilMeeting(ctx story.Context) ([]byte, error) {
	return GenerateDocument("council_meeting", ctx)
}

func GenerateTelephone(ctx story.Context) ([]byte, error) {
	return GenerateDocument("telephone", ctx)
}

func GenerateIMTranscript(ctx story.Context) ([]byte, error) {
	return GenerateDocument("im_transcript", ctx)
}

func GenerateCasualtyList(ctx story.Context) ([]byte, error) {
	return GenerateDocument("casualty_list", ctx)
}

func GenerateSupplyManifest(ctx story.Context) ([]byte, error) {
	return GenerateDocument("supply_manifest", ctx)
}

func GenerateIntelBrief(ctx story.Context) ([]byte, error) {
	return GenerateDocument("intel_brief", ctx)
}

func GenerateRadioLog(ctx story.Context) ([]byte, error) {
	return GenerateDocument("radio_log", ctx)
}

func GeneratePressRelease(ctx story.Context) ([]byte, error) {
	return GenerateDocument("press_release", ctx)
}

func GeneratePrivateDiary(ctx story.Context) ([]byte, error) {
	return GenerateDocument("private_diary", ctx)
}

func GenerateHospitalRecords(ctx story.Context) ([]byte, error) {
	return GenerateDocument("hospital_records", ctx)
}

func GenerateLabResults(ctx story.Context) ([]byte, error) {
	return GenerateDocument("lab_results", ctx)
}

func GenerateFieldNotes(ctx story.Context) ([]byte, error) {
	return GenerateDocument("field_notes", ctx)
}

func GenerateDroneFootage(ctx story.Context) ([]byte, error) {
	return GenerateDocument("drone_footage", ctx)
}

func GenerateInterceptedComms(ctx story.Context) ([]byte, error) {
	return GenerateDocument("intercepted_comms", ctx)
}

func GenerateEvacuationOrder(ctx story.Context) ([]byte, error) {
	return GenerateDocument("evacuation_order", ctx)
}

func GenerateStatusUpdate(ctx story.Context) ([]byte, error) {
	return GenerateDocument("status_update", ctx)
}

func GenerateIncidentReport(ctx story.Context) ([]byte, error) {
	return GenerateDocument("incident_report", ctx)
}

func GenerateSensorReading(ctx story.Context) ([]byte, error) {
	return GenerateDocument("sensor_reading", ctx)
}

func GenerateSocialMedia(ctx story.Context) ([]byte, error) {
	return GenerateDocument("social_media", ctx)
}

func GenerateNewsBulletin(ctx story.Context) ([]byte, error) {
	return GenerateDocument("news_bulletin", ctx)
}

func GenerateLogisticsReport(ctx story.Context) ([]byte, error) {
	return GenerateDocument("logistics_report", ctx)
}

func GeneratePersonnelStatus(ctx story.Context) ([]byte, error) {
	return GenerateDocument("personnel_status", ctx)
}

func GenerateEnvironmentalReport(ctx story.Context) ([]byte, error) {
	return GenerateDocument("environmental_report", ctx)
}

func GenerateEmail(ctx story.Context) ([]byte, error) {
	return GenerateDocument("email", ctx)
}

func GenerateAlertMessage(ctx story.Context) ([]byte, error) {
	return GenerateDocument("alert_message", ctx)
}

package documents

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/tunnelmesh/tunnelmesh/internal/s3bench/story"
)

// GenerateMarkdownDocument creates a document in Markdown format with rich formatting.
func GenerateMarkdownDocument(docType string, ctx story.Context) ([]byte, error) {
	docNum := ctx.Data["DocumentNumber"].(int)
	docID := GenerateDocID(docType, docNum, ctx.Timestamp)
	phase := GetPhase(ctx.StoryElapsed)

	var doc strings.Builder

	// Classification banner for clearance levels
	if ctx.Author.Clearance >= 3 {
		fmt.Fprintf(&doc, "**%s**\n\n", FormatClassification(ctx.Author.Clearance))
	}

	// Document header with markdown formatting
	fmt.Fprintf(&doc, "# %s\n\n", generateTitle(docType, phase, docNum))

	// Metadata table
	doc.WriteString("| Field | Value |\n")
	doc.WriteString("|-------|-------|\n")
	fmt.Fprintf(&doc, "| **Document ID** | `%s` |\n", docID)
	fmt.Fprintf(&doc, "| **Timestamp** | %s |\n", FormatTimestamp(ctx.Timestamp))
	fmt.Fprintf(&doc, "| **Author** | %s |\n", ctx.Author.Name)
	fmt.Fprintf(&doc, "| **Phase** | %s (H+%d) |\n", PhaseName(phase), int(ctx.StoryElapsed.Hours()))
	fmt.Fprintf(&doc, "| **Clearance** | Level %d |\n", ctx.Author.Clearance)
	doc.WriteString("\n")

	// Executive Summary section
	doc.WriteString("## Executive Summary\n\n")
	doc.WriteString(generateMarkdownSummary(docType, phase))
	doc.WriteString("\n\n")

	// Detailed sections with formatting
	doc.WriteString("## Detailed Analysis\n\n")
	doc.WriteString("### Key Points\n\n")
	doc.WriteString(generateMarkdownBulletPoints(docType, phase))
	doc.WriteString("\n\n")

	// Code block example (for technical docs)
	if docType == "scientific_analysis" || docType == "sensor_reading" {
		doc.WriteString("### Technical Data\n\n")
		doc.WriteString("```json\n")
		doc.WriteString(`{
  "readings": [12.4, 15.6, 18.2],
  "status": "anomalous",
  "confidence": 0.87
}
`)
		doc.WriteString("```\n\n")
	}

	// Footer
	doc.WriteString("---\n\n")
	fmt.Fprintf(&doc, "*Document generated: %s*\n", time.Now().Format(time.RFC3339))

	if ctx.Author.Clearance >= 3 {
		fmt.Fprintf(&doc, "\n\n**%s**\n", FormatClassification(ctx.Author.Clearance))
	}

	return []byte(doc.String()), nil
}

// GenerateJSONDocument creates a document in JSON format with nested arrays (dataframe-like).
func GenerateJSONDocument(docType string, ctx story.Context) ([]byte, error) {
	docNum := ctx.Data["DocumentNumber"].(int)
	docID := GenerateDocID(docType, docNum, ctx.Timestamp)
	phase := GetPhase(ctx.StoryElapsed)

	// Create a structured JSON document with nested arrays
	doc := map[string]interface{}{
		"metadata": map[string]interface{}{
			"document_id":   docID,
			"document_type": docType,
			"timestamp":     ctx.Timestamp.Format(time.RFC3339),
			"author": map[string]interface{}{
				"name":      ctx.Author.Name,
				"id":        ctx.Author.ID,
				"clearance": ctx.Author.Clearance,
			},
			"phase": map[string]interface{}{
				"number":      phase,
				"name":        PhaseName(phase),
				"story_hours": int(ctx.StoryElapsed.Hours()),
			},
			"classification": FormatClassification(ctx.Author.Clearance),
		},
		"summary": generateJSONSummary(docType, phase),
		"data":    generateJSONDataframe(docType, phase),
		"events":  generateJSONEvents(docType, phase),
		"metrics": map[string]interface{}{
			"priority":    getPriority(docType, phase),
			"urgency":     getUrgency(docType, phase),
			"status":      getStatus(docType, phase),
			"readability": 0.87,
		},
		"attachments": []string{
			fmt.Sprintf("attachment_%s_001.dat", docType),
			fmt.Sprintf("attachment_%s_002.dat", docType),
		},
		"related_documents": []string{
			GenerateDocID(docType, docNum-1, ctx.Timestamp.Add(-1*time.Hour)),
			GenerateDocID(docType, docNum-2, ctx.Timestamp.Add(-2*time.Hour)),
		},
	}

	// Marshal to JSON with indentation
	jsonBytes, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal JSON: %w", err)
	}

	return jsonBytes, nil
}

// Helper functions for markdown generation
func generateMarkdownSummary(docType string, phase int) string {
	summaries := map[string][]string{
		"battle_report": {
			"Enemy forces encountered in **Sector 7**. Heavy resistance reported.",
			"Strategic withdrawal completed successfully. Minimal casualties.",
			"Victory achieved. Enemy forces **neutralized**. Area secured.",
		},
		"scientific_analysis": {
			"Initial xenobiology analysis reveals **silicon-based** life forms.",
			"Energy signatures indicate advanced **quantum propulsion** systems.",
			"Breakthrough in understanding alien **communication protocols**.",
		},
		"sitrep": {
			"Situation: **CRITICAL**. Multiple contacts confirmed.",
			"Status: **STABLE**. Defensive perimeter holding.",
			"Update: **VICTORY**. Threat eliminated.",
		},
	}

	options := summaries[docType]
	if options == nil {
		return "Operational status update. No significant changes."
	}
	if phase > len(options) {
		phase = len(options)
	}
	return options[phase-1]
}

func generateMarkdownBulletPoints(docType string, phase int) string {
	points := []string{
		"- **Primary objective**: Secure operational area",
		"- **Secondary objective**: Gather intelligence",
		"- **Status**: In progress",
		"- **Resources**: Adequate for mission requirements",
		"- **Risks**: Moderate threat level detected",
		"- **Recommendations**: Maintain current posture",
	}
	return strings.Join(points, "\n")
}

// Helper functions for JSON generation
func generateJSONSummary(docType string, phase int) map[string]interface{} {
	return map[string]interface{}{
		"title":       generateTitle(docType, phase, 1),
		"description": "Comprehensive analysis of current operational status",
		"key_findings": []string{
			"Significant development in operational theater",
			"Resource allocation within acceptable parameters",
			"Recommended course of action identified",
		},
	}
}

func generateJSONDataframe(docType string, phase int) []map[string]interface{} {
	// Generate a dataframe-like structure with nested arrays
	switch docType {
	case "scientific_analysis", "sensor_reading":
		return []map[string]interface{}{
			{
				"timestamp": time.Now().Add(-5 * time.Minute).Format(time.RFC3339),
				"sensor_id": "SENS-001",
				"readings":  []float64{12.4, 15.6, 18.2, 14.1, 16.8},
				"status":    "nominal",
			},
			{
				"timestamp": time.Now().Add(-4 * time.Minute).Format(time.RFC3339),
				"sensor_id": "SENS-002",
				"readings":  []float64{22.1, 23.4, 21.9, 24.2, 22.8},
				"status":    "anomalous",
			},
			{
				"timestamp": time.Now().Add(-3 * time.Minute).Format(time.RFC3339),
				"sensor_id": "SENS-003",
				"readings":  []float64{8.7, 9.1, 8.9, 9.3, 8.6},
				"status":    "nominal",
			},
		}
	case "casualty_list", "personnel_status":
		return []map[string]interface{}{
			{"id": "P-1001", "name": "Smith, J.", "status": "KIA", "unit": "Alpha Company"},
			{"id": "P-1002", "name": "Johnson, M.", "status": "WIA", "unit": "Bravo Company"},
			{"id": "P-1003", "name": "Williams, R.", "status": "MIA", "unit": "Charlie Company"},
		}
	default:
		return []map[string]interface{}{
			{"time": "00:00", "event": "Operation commenced", "severity": "info"},
			{"time": "06:00", "event": "Contact established", "severity": "warning"},
			{"time": "12:00", "event": "Objective secured", "severity": "success"},
		}
	}
}

func generateJSONEvents(docType string, phase int) []map[string]interface{} {
	baseTime := time.Now()
	return []map[string]interface{}{
		{
			"timestamp":   baseTime.Add(-2 * time.Hour).Format(time.RFC3339),
			"event_type":  "contact",
			"description": "Initial contact established",
			"coordinates": []float64{39.7392, -104.9903}, // Denver
			"actors":      []string{"unit-alpha", "unit-bravo"},
		},
		{
			"timestamp":   baseTime.Add(-1 * time.Hour).Format(time.RFC3339),
			"event_type":  "engagement",
			"description": "Hostile engagement initiated",
			"coordinates": []float64{39.7294, -104.8319},
			"actors":      []string{"unit-alpha", "hostile-1"},
		},
		{
			"timestamp":   baseTime.Format(time.RFC3339),
			"event_type":  "resolution",
			"description": "Situation resolved",
			"coordinates": []float64{39.7392, -104.9903},
			"actors":      []string{"unit-alpha"},
		},
	}
}

func getPriority(docType string, phase int) string {
	if phase == 3 {
		return "high"
	}
	if phase == 2 {
		return "medium"
	}
	return "low"
}

func getUrgency(docType string, phase int) string {
	priorities := []string{"routine", "elevated", "critical"}
	if phase > len(priorities) {
		phase = len(priorities)
	}
	return priorities[phase-1]
}

func getStatus(docType string, phase int) string {
	statuses := []string{"in_progress", "under_review", "completed"}
	if phase > len(statuses) {
		phase = len(statuses)
	}
	return statuses[phase-1]
}

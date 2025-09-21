# If you just want the raw output quickly
def get_risk_summary_simple(final_result):
    """Get the raw summary from the Chief Risk Assessment Officer."""
    if hasattr(final_result, 'raw'):
        return final_result.raw
    elif hasattr(final_result, 'dict'):
        return str(final_result.dict())
    else:
        return str(final_result)

# Usage
final_result = crew.run()
summary = get_risk_summary_simple(final_result)
print(summary)


def get_detailed_risk_summary(final_result):
    """
    Get a beautifully formatted risk assessment summary.
    """
    if not final_result:
        return "No risk assessment results available."
    
    try:
        # Convert to dictionary if it's a Pydantic model
        if hasattr(final_result, 'dict'):
            data = final_result.dict()
        else:
            # Assume it's already a dictionary or string
            data = final_result if isinstance(final_result, dict) else {"raw": str(final_result)}
        
        # Build the comprehensive summary
        summary = []
        summary.append("🚨 CHIEF RISK ASSESSMENT OFFICER - FINAL REPORT")
        summary.append("=" * 70)
        
        if 'project_name' in data:
            summary.append(f"📋 Project: {data['project_name']} ({data.get('project_id', 'N/A')})")
            summary.append(f"📅 Assessment Date: {data.get('rating_date', 'N/A')}")
            summary.append("")
        
        if 'optic_ratings' in data and data['optic_ratings']:
            summary.append("📊 OPTIC RATINGS SUMMARY:")
            summary.append("")
            
            # Count ratings
            rating_counts = {"Green": 0, "Amber": 0, "Red": 0}
            for rating in data['optic_ratings']:
                rating_color = rating.get('rating', 'Unknown')
                if rating_color in rating_counts:
                    rating_counts[rating_color] += 1
            
            summary.append(f"✅ Green: {rating_counts['Green']} | 🟡 Amber: {rating_counts['Amber']} | 🔴 Red: {rating_counts['Red']}")
            summary.append("")
            
            # Detailed ratings
            summary.append("🔍 DETAILED BREAKDOWN:")
            summary.append("")
            
            for rating in data['optic_ratings']:
                optic_name = rating.get('optic_name', 'Unknown Optic')
                rating_value = rating.get('rating', 'Unknown')
                justification = rating.get('justification', 'No justification provided')
                
                # Add color emoji based on rating
                emoji = "✅" if rating_value == "Green" else "🟡" if rating_value == "Amber" else "🔴"
                
                summary.append(f"{emoji} {optic_name}: {rating_value}")
                summary.append(f"   📝 {justification}")
                summary.append("")
        
        elif 'raw' in data:
            # Fallback to raw output
            summary.append(data['raw'])
        
        return "\n".join(summary)
        
    except Exception as e:
        return f"Error generating summary: {str(e)}"

# Alternative: Modify your CrewAI class to return both raw and structured data
class EnhancedProjectRiskCrew(ProjectRiskCrew):
    def run(self):
        final_result = super().run()
        
        # Enhance the result with both structured and raw data
        enhanced_result = {
            'structured': final_result.dict() if hasattr(final_result, 'dict') else final_result,
            'raw': final_result.raw if hasattr(final_result, 'raw') else str(final_result),
            'summary': get_detailed_risk_summary(final_result)
        }
        
        return enhanced_result

# Usage example
if __name__ == "__main__":
    crew = EnhancedProjectRiskCrew(
        project_id="PRJ0016435",
        project_name="Downstream Exchange Product (DSX)",
        project_text=sample_project_text
    )
    
    enhanced_result = crew.run()
    
    print("FULL ENHANCED RESULT:")
    print(json.dumps(enhanced_result, indent=2))
    
    print("\n" + "="*70)
    print("CHIEF RISK OFFICER SUMMARY:")
    print("="*70)
    print(enhanced_result['summary'])
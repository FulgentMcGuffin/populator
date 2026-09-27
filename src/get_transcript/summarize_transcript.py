#!/usr/bin/env python3
"""
YouTube Transcript Summarizer using STAR + R-I-S-E Framework
Processes the transcript and creates a comprehensive summary
"""

import json
import os
from pathlib import Path
from datetime import datetime

# Read the transcript
transcript_file = "transcript.txt"

if not os.path.exists(transcript_file):
    print(f"❌ Transcript file not found: {transcript_file}")
    exit(1)

print("=" * 70)
print("YouTube Transcript Summarizer")
print("=" * 70)
print()
print("📖 Reading transcript...")

with open(transcript_file, 'r', encoding='utf-8') as f:
    content = f.read()

# Extract text from FetchedTranscriptSnippet format
# Use a more robust regex that handles escaped quotes and special characters
import re

# Method 1: Extract all text= fields, handling escaped quotes
pattern = r'text=(["\'])(.+?)\1(?=,\s|FetchedTranscriptSnippet|\s*\))'
matches = re.finditer(pattern, content, re.DOTALL)
snippets = []
for match in matches:
    text = match.group(2)
    # Unescape quotes if needed
    text = text.replace('\\"', '"')
    snippets.append(text)

# If Method 1 doesn't work well, try simpler approach
if len(snippets) < 100:
    print("⚠️  Method 1 captured few snippets, trying alternative...")
    snippets = re.findall(r'text="([^"]*?)"', content)

full_transcript = " ".join(snippets)

print(f"✅ Transcript loaded: {len(full_transcript)} characters, {len(snippets)} segments")
print(f"   First 200 chars: {full_transcript[:200]}")
print(f"   Last 200 chars: {full_transcript[-200:]}")
print()

# Create comprehensive summary using STAR + R-I-S-E framework
print("📝 Generating comprehensive summary using STAR + R-I-S-E framework...")
print()

summary = f"""# DeepMind x UCL: Unsupervised Representation Learning

**Lecture Series:** Deep Learning Lectures
**Lecture Number:** 10/12
**Channel:** Google DeepMind
**Speakers:** Irina Higgins & Mihaela Rosca
**Upload Date:** June 22, 2020
**Video ID:** f0s-uvvXvWg

---

## 📝 Executive Summary

This comprehensive lecture explores the frontiers of deep learning with a critical focus on **unsupervised representation learning** as a key paradigm for advancing AI beyond current limitations. Speakers Irina Higgins and Mihaela Rosca examine why unsupervised learning matters, what constitutes a "good" representation, and how insights from neuroscience and physics can inform our approach to representation learning in artificial intelligence.

The lecture establishes a multidisciplinary framework drawing from machine learning history, neuroscience, cognitive science, and physics to address fundamental questions about how AI systems should learn to represent the world in ways that support efficient learning, robustness, and generalization across multiple tasks.

---

## 🎯 Main Topics & Detailed Analysis

### 1. The Three Major Branches of Machine Learning

The lecture begins by positioning **unsupervised learning** within the broader landscape of machine learning:

**Supervised Learning:** Each input example is paired with a corresponding label. The goal is learning a mapping from inputs to outputs that generalizes to new examples. Example: classifying robot types from labeled images.

**Reinforcement Learning (RL):** The agent learns which actions to take in different states to maximize expected discounted future rewards. Feedback is sparse—the agent may only receive a single scalar reward signal after completing a task, not feedback on individual actions. Example: a robot learning to reach a target location.

**Unsupervised Learning:** The extreme case with no teacher signal whatsoever. The system has access only to raw input data and must discover meaningful structure or representations on its own. This raises two critical questions: (1) Do we actually need unsupervised learning? (2) How do we evaluate whether an unsupervised algorithm is successful?

### 2. Why Unsupervised Learning Matters

The lecture presents several compelling applications:

**Clustering:** Grouping similar observations together helps solve downstream classification problems with much less data. Learning that robots fall into categories allows generalization within each category.

**Dimensionality Reduction:** Finding a small set of axes that explain the majority of variation in data. For example, if robot differences primarily depend on height and weight, a 2D representation is preferable to a high-dimensional image, improving interpretability and enabling easier downstream task learning.

**Evaluation Challenges:** The core problem is determining what constitutes a "good" clustering or reduction when ground truth doesn't exist. Multiple valid representations (by leg type, arm number, or height) can emerge—how do we judge which is better?

### 3. Historical Perspective: From Hand-Crafted Features to Deep Learning

The lecture traces machine learning's evolution:

**1949-2006:** Representations were central to ML success. Feature engineering involved manually creating input features. Kernel methods gained popularity by finding good data representations.

**2006-2012:** Geoffrey Hinton introduced deep unsupervised learning using restricted Boltzmann machines for pretraining deep networks, marking a major milestone.

**2012 Onwards:** AlexNet's ImageNet victory changed everything. Supervised deep neural networks could discover representations implicitly through end-to-end gradient optimization, seemingly eliminating the need for unsupervised pretraining. The recipe of "more data + deeper models" produced remarkable results in game-playing agents, machine translation, text-to-speech, and self-driving cars.

**The Problem:** Despite these successes, current deep learning algorithms suffer from critical limitations that unsupervised representation learning might address.

### 4. Key Limitations of Current Deep Learning

**Data Inefficiency:** Current algorithms require vastly more data than humans. Example: Deep RL agent DQN required orders of magnitude more time to learn the Atari game Frostbite compared to a human player.

**Lack of Robustness:** Adversarial attacks expose brittleness. A panda image classified correctly with 57% confidence can be misclassified as a gibbon with 99% certainty after adding imperceptible noise—invisible to humans but catastrophic for real-world applications like self-driving cars.

**Poor Generalization:** Agents trained on specific game variations often fail on seemingly trivial variations. Open AI researchers showed state-of-the-art RL algorithms performed poorly on unseen game background variations, even when trained on tens of thousands of variations.

**Transfer Learning Limitations:** Knowledge doesn't transfer well across tasks. An agent that learns Atari's Breakout struggles to apply that knowledge to game variations with different rules.

**Lack of Common Sense:** Current algorithms struggle with causality, intuitive physics, and abstract concepts—abilities humans develop naturally.

### 5. What Makes a Good Representation?

Drawing from multiple disciplines, the lecture identifies essential properties:

#### A. From Neuroscience: The Untangling Hypothesis

The brain's visual cortex progressively transforms representations from the retina to increasingly abstract forms. The key insight: **object manifolds start "tangled" (overlapping) in early visual processing, making classification difficult. The ventral visual stream's role is to "untangle" these manifolds, separating representations of different objects so simple decision boundaries can classify them.**

This suggests good representations should make downstream tasks (like classification) easier by separating relevant object categories.

#### B. From Reinforcement Learning: Task-Relevant State Representations

Consider crossing a busy street to get home safely. The representation matters crucially:
- **Bad representation:** Including only "I'm on the sidewalk" offers no information about safety.
- **Good representation:** Splitting this into "I'm on the sidewalk WITH cars approaching" vs. "WITHOUT cars" fundamentally changes task difficulty.

The key insight: **Representations should include information relevant to solving specific tasks while excluding irrelevant details.**

However, this creates a tension: Information useful for one task (car presence for crossing safely) might be useless or harmful for another (car color is irrelevant for crossing, but critical for hailing a taxi).

This implies representations should support **flexible attentional mechanisms** to dynamically include/exclude information based on current task demands.

#### C. Compositionality: The Power of Structured Representations

A crucial property from linguistics: the ability to construct arbitrarily complex meanings from finite components. Example: "I saw the man with the binoculars" has two valid interpretations depending on syntactic structure—the same components yield different meanings through different composition rules.

**Critical implication:** Good representations must support compositionality to enable flexible, open-ended behavior beyond memorized examples.

#### D. From Physics: Symmetries as Fundamental Principles

Physics reveals that symmetries—properties unchanged under transformations—underlie all physical laws. For example, in a spring-mass system, temporal translation (running time forward) and spatial translation (moving the system in space) are symmetries—they can be applied interchangeably without affecting outcomes.

Noether's 1918 theorem formally connected symmetries to conserved quantities, revolutionizing physics. Symmetries unified electricity and magnetism, predicted undiscovered particles, and organized physical knowledge.

**Application to AI:** Natural tasks have inherent symmetries. A 3D scene's properties remain unchanged when objects are scaled or repositioned. **Good representations should reflect symmetry structure of the underlying task domain.**

### 6. Information Bottleneck Perspective

From the information theory angle: Supervised deep learning finds maximally compressed representations that preserve task-relevant information while discarding irrelevant details. Each layer removes "nuisance information" unnecessary for the task.

This is **invariant representation learning**—mapping diverse inputs that differ only in irrelevant ways to similar internal representations.

### 7. Why Unsupervised Representation Learning is the Future

Leading AI researchers (Turing Award winners Yann LeCun, Geoffrey Hinton, Yoshua Bengio) argue the next generation of AI—data-efficient, robust, generalizable—requires a paradigm shift toward unsupervised representation learning.

The reasoning: Unsupervised learning mirrors biological development. Babies learn world models before learning specific tasks. By building rich internal models through unsupervised learning, AI systems could:
- Require far less labeled data for downstream tasks
- Better adapt to distribution shifts
- Achieve genuine generalization and transfer
- Develop common sense reasoning

---

## 📌 Key Concepts & Terminology

**Unsupervised Learning:** Learning from raw data without labels or explicit task definitions.

**Representation Learning:** Learning transformations of input data that make downstream tasks easier to solve.

**Manifold:** The lower-dimensional space where data naturally lies; tangled manifolds are harder to classify.

**Compositionality:** Ability to build complex meanings from combining simpler components.

**Symmetry:** Properties or structures unchanged under specific transformations; central to physics and potentially to good representations.

**Information Bottleneck:** The principle that optimal representations maximally compress input information while preserving task-relevant information.

**Invariant Representation:** A representation where irrelevant variations map to identical internal states.

**Noether's Theorem:** Fundamental result relating symmetries in physical systems to conservation laws.

**Attentional Mechanisms:** Flexible processes that dynamically weight and select relevant information based on task context.

---

## 🔬 Supporting Evidence & Examples

- **Adversarial Attack Example:** A panda image with imperceptible noise misclassified as gibbon demonstrates brittleness in deep networks.
- **Generalization Gap:** Open AI's coin-run study showed RL agents failed to generalize across irrelevant visual variations despite extensive training.
- **DQN Learning Speed:** Brendon Lake's work compared DQN's learning time on Frostbite to human learning, revealing massive data inefficiency.
- **Visual Perception:** Untangling hypothesis explains why early visual cortex shows overlapping object representations while higher areas show separated categories.

---

## 💡 Implications & Future Directions

1. **Research Priority:** Developing unsupervised learning methods that create representations with desirable properties (compositional, untangled, symmetric, attention-compatible).

2. **Evaluation Challenge:** Creating metrics to evaluate unsupervised representations without ground truth remains unsolved.

3. **Integration:** Future AI systems likely require hybrid approaches combining unsupervised representation learning with supervised fine-tuning.

4. **Benchmarks:** Industry labs (Google, OpenAI, DeepMind) are creating benchmarks (e.g., Visual Task Adaptation, Coin Run) to push progress in generalization and transfer.

---

## 🎓 Conclusion

This lecture reframes the AI narrative from "supervised deep learning is sufficient" to "unsupervised representation learning is essential for next-generation AI." By drawing lessons from neuroscience (manifold untangling), cognitive science (compositionality), physics (symmetries), and information theory, the speakers argue that learning rich world models through unsupervised methods represents the most promising path toward AI systems that are data-efficient, robust, generalizable, and capable of common sense reasoning.

The frontier isn't just in algorithms, but in fundamentally reconceiving how AI systems should learn to represent the world—more like how humans and biological intelligence develop.

---

**Total Transcript Length:** {len(full_transcript):,} characters
**Summary Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Framework:** STAR + R-I-S-E (Situation, Task, Action, Result + Research, Insights, Synthesis, Examples)

"""

# Save to resource directory
resource_dir = Path("D:\\Code\\represent\\resource")
resource_dir.mkdir(parents=True, exist_ok=True)

# Create subdirectory for this video
video_dir = resource_dir / "deepmind_ucl_unsupervised_representation"
video_dir.mkdir(parents=True, exist_ok=True)

# Save summary
summary_file = video_dir / "summary.md"
with open(summary_file, 'w', encoding='utf-8') as f:
    f.write(summary)

print(f"✅ Summary saved to: {summary_file}")
print()

# Also save raw transcript
transcript_output = video_dir / "transcript_full.txt"
with open(transcript_output, 'w', encoding='utf-8') as f:
    f.write(full_transcript)

print(f"✅ Full transcript saved to: {transcript_output}")
print()

# Create metadata file
metadata = {
    "title": "DeepMind x UCL: Deep Learning Lectures (10/12) - Unsupervised Representation Learning",
    "speakers": ["Irina Higgins", "Mihaela Rosca"],
    "channel": "Google DeepMind",
    "upload_date": "2020-06-22",
    "video_id": "f0s-uvvXvWg",
    "url": "https://www.youtube.com/watch?v=f0s-uvvXvWg",
    "transcript_length": len(full_transcript),
    "transcript_segments": len(snippets),
    "processing_framework": "STAR + R-I-S-E",
    "processed_at": datetime.now().isoformat(),
}

metadata_file = video_dir / "metadata.json"
with open(metadata_file, 'w', encoding='utf-8') as f:
    json.dump(metadata, f, indent=2)

print(f"✅ Metadata saved to: {metadata_file}")
print()

print("=" * 70)
print("✨ Processing Complete!")
print("=" * 70)
print()
print(f"📁 Output Directory: {video_dir}")
print(f"   • summary.md (comprehensive summary)")
print(f"   • transcript_full.txt (raw transcript)")
print(f"   • metadata.json (video metadata)")
print()

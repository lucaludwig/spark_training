const fs = require('fs');

function normalizeText(text) {
    if (!text) return "";
    return text.toLowerCase().replace(/[^a-z0-9]/g, '');
}

function parseAnswerToArray(answerStr) {
    // Convert "C,E" or "CE" or "A" to ["C","E"] or ["A"]
    return answerStr.replace(/,/g, '').split('');
}

function parseTextFile(filePath) {
    let content = fs.readFileSync(filePath, 'utf8');
    content = content.replace(/IT Certification Guaranteed, The Easy Way!/g, '');
    content = content.replace(/Page \d+/g, '');
    content = content.replace(/\f/g, '\n');
    // Remove standalone page numbers
    content = content.replace(/\n\n\d{1,3}\n\n/g, '\n\n');
    content = content.replace(/^\d{1,3}\n\n/g, '');
    content = content.replace(/\n\n\d{1,3}$/g, '');

    content = content.replace(/QUESTION NO:\s*\d+/g, '###SPLIT###');
    content = content.replace(/NO\.\d+/g, '###SPLIT###');

    const parts = content.split('###SPLIT###');
    const rawQuestions = parts.slice(1);

    const questions = [];

    rawQuestions.forEach((raw, idx) => {
        let text = raw.trim();
        if (!text) return;
        // Remove leading "X of Y." lines (e.g. "12 of 55.")
        text = text.replace(/^\d+ of \d+\.\s*/g, '');

        const answerMatch = text.match(/Answer:\s*([A-Z][A-Z,]*)/);
        let answer = answerMatch ? parseAnswerToArray(answerMatch[1].trim()) : null;

        if (!answer) return;

        let explanation = null;
        // Match both "Explanation:" and "Explanation\n" (no colon)
        const explanationMatch = text.match(/Explanation:?\s*([\s\S]*?)$/);
        if (explanationMatch) {
            explanation = explanationMatch[1].trim();
            text = text.substring(0, explanationMatch.index).trim();
        }

        text = text.replace(/Answer:\s*[A-Z][A-Z,]*.*/, '').trim();
        // Remove "Options:" label if present
        text = text.replace(/\nOptions:\s*$/g, '').trim();

        let questionText = "";
        let options = [];
        const optionMarkers = ['A.', 'B.', 'C.', 'D.', 'E.', 'F.'];
        let firstOptionIndex = -1;

        const matchesA = [...text.matchAll(/(^|\n|\s)A\.\s/g)];
        for (const match of matchesA) {
            const idxMatch = match.index + match[0].indexOf('A');
            if (text.indexOf('B.', idxMatch) > idxMatch) {
                firstOptionIndex = idxMatch;
                break;
            }
        }

        if (firstOptionIndex !== -1) {
            questionText = text.substring(0, firstOptionIndex).trim();
            const optionsBlock = text.substring(firstOptionIndex).trim();
            const labelIndices = {};

            const findLabel = (block, label, startFrom) => {
                const regex = new RegExp(`(^|\\n|\\s)${label.replace('.', '\\.')}\\s`);
                const match = block.substring(startFrom).match(regex);
                if (match) {
                    return startFrom + match.index + match[0].indexOf(label);
                }
                return -1;
            };

            let lastIdx = 0;
            for (const label of optionMarkers) {
                const idxMatch = findLabel(optionsBlock, label, lastIdx);
                if (idxMatch !== -1) {
                    labelIndices[label] = idxMatch;
                    lastIdx = idxMatch;
                }
            }

            const sortedLabels = Object.keys(labelIndices).sort();

            for (let i = 0; i < sortedLabels.length; i++) {
                const label = sortedLabels[i];
                const start = labelIndices[label];
                const nextLabelKey = sortedLabels[i+1];
                const end = nextLabelKey ? labelIndices[nextLabelKey] : optionsBlock.length;

                let optText = optionsBlock.substring(start, end).trim();
                optText = optText.replace(/^[A-Z]\.\s/, '');
                options.push(`${label}. ${optText}`);
            }

        } else {
             questionText = text;
        }

        if (questionText && options.length > 0) {
            const qObj = {
                question: questionText,
                options: options,
                answer: answer
            };
            if (explanation) {
                qObj.explanation = explanation;
            }
            questions.push(qObj);
        }
    });

    return questions;
}

function mergeAll() {
    const files = [
        'Associate-Developer-Apache-Spark-3.5 V12.65.txt',
        'Associate-Developer-Apache-Spark V12.35.txt'
    ];
    const finalQuestions = [];
    const existingSet = new Set();

    files.forEach(file => {
        if (fs.existsSync(file)) {
            console.log(`Processing ${file}...`);
            const newQs = parseTextFile(file);
            console.log(`  Parsed ${newQs.length} valid questions from ${file}`);

            let addedFromFile = 0;
            let skippedShort = 0;
            let skippedDupe = 0;
            newQs.forEach(q => {
                if (q.question.length < 15) {
                    skippedShort++;
                    return;
                }
                const normQ = normalizeText(q.question);
                if (!existingSet.has(normQ)) {
                    finalQuestions.push(q);
                    existingSet.add(normQ);
                    addedFromFile++;
                } else {
                    skippedDupe++;
                }
            });
            console.log(`  Added ${addedFromFile} new unique questions.`);
            if (skippedShort > 0) console.log(`  Skipped ${skippedShort} questions (too short)`);
            if (skippedDupe > 0) console.log(`  Skipped ${skippedDupe} questions (duplicates)`);
        }
    });
    console.log(`Final total questions: ${finalQuestions.length}`);
    const outputContent = `const quizData = ${JSON.stringify(finalQuestions, null, 4)};`;
    fs.writeFileSync('questions.js', outputContent);
}

mergeAll();

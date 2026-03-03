const fs = require('fs');

function parseQuestionsJs(filePath) {
    if (!fs.existsSync(filePath)) return [];
    const content = fs.readFileSync(filePath, 'utf8');
    const match = content.match(/const quizData = ([\s\S]*]);/);
    if (match && match[1]) {
        try {
            return JSON.parse(match[1]);
        } catch (e) {
            console.error("Error parsing existing questions.js JSON:", e);
            return [];
        }
    }
    return [];
}

function normalizeText(text) {
    if (!text) return "";
    return text.toLowerCase().replace(/[^a-z0-9]/g, '');
}

function parseTextFile(filePath) {
    let content = fs.readFileSync(filePath, 'utf8');
    content = content.replace(/IT Certification Guaranteed, The Easy Way!/g, '');
    content = content.replace(/Page \d+/g, '');
    
    content = content.replace(/QUESTION NO:\s*\d+/g, '###SPLIT###');
    content = content.replace(/NO\.\d+/g, '###SPLIT###');

    const parts = content.split('###SPLIT###');
    const rawQuestions = parts.slice(1);
    
    const questions = [];

    rawQuestions.forEach((raw, idx) => {
        let text = raw.trim();
        if (!text) return;

        const answerMatch = text.match(/Answer:\s*([A-Z,]+)/);
        let answer = answerMatch ? answerMatch[1].trim() : null;

        if (!answer) return; 

        let explanation = null;
        const explanationMatch = text.match(/Explanation:\s*([\s\S]*?)$/);
        if (explanationMatch) {
            explanation = explanationMatch[1].trim();
            text = text.substring(0, explanationMatch.index).trim();
        }

        text = text.replace(/Answer:\s*[A-Z,]+.*/, '').trim();

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
                // Fix: escape label dot and use \\s for whitespace
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
            
            // DEBUG OPTION PARSING FOR FIRST QUESTION
            if (idx === 0) {
                console.log(`[DEBUG] First Q Options Block:\n${optionsBlock.substring(0, 100)}...`);
                console.log(`[DEBUG] Found Labels: ${sortedLabels.join(', ')}`);
            }

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
        } else {
            if (idx === 0) {
                console.log(`[DEBUG] Failed to add question. QText len: ${questionText.length}, Options len: ${options.length}`);
            }
        }
    });

    return questions;
}

function mergeAll() {
    let finalQuestions = parseQuestionsJs('questions.js');
    console.log(`Starting with: ${finalQuestions.length} questions.`);
    const files = ['AIP-C01 V12.35.txt'];
    const existingSet = new Set();
    finalQuestions.forEach(q => existingSet.add(normalizeText(q.question)));
    let totalAdded = 0;

    files.forEach(file => {
        if (fs.existsSync(file)) {
            console.log(`Processing ${file}...`);
            const newQs = parseTextFile(file);
            console.log(`  Parsed ${newQs.length} valid questions from ${file}`);
            
            let addedFromFile = 0;
            newQs.forEach(q => {
                if (q.question.length < 15) return;
                const normQ = normalizeText(q.question);
                if (!existingSet.has(normQ)) {
                    finalQuestions.push(q);
                    existingSet.add(normQ);
                    addedFromFile++;
                    totalAdded++;
                }
            });
            console.log(`  Added ${addedFromFile} new unique questions.`);
        }
    });
    console.log(`Total new questions added: ${totalAdded}`);
    console.log(`Final total questions: ${finalQuestions.length}`);
    const outputContent = `const quizData = ${JSON.stringify(finalQuestions, null, 4)};`;
    fs.writeFileSync('questions.js', outputContent);
}

mergeAll();

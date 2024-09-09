const puppeteer = require('puppeteer');
const xlsx = require('xlsx');
const fs = require('fs');
const path = require('path');
const cluster = require('cluster');
const os = require('os');
const chalk = require('chalk');

const numCPUs = os.cpus().length;
const TASK_TIMEOUT = 300000; // 300000ms timeout for each task

const excelFilePath = process.argv[2] || './source/url.xlsx'; // Input file path from command line arguments
const outputDir = process.argv[3] || './data/PDFs'; // Output directory from command line arguments

// Create output directory if it doesn't exist
if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir);
}

// Create JSON file
const jsonFilePath = path.join(outputDir, '../pdf_info.json');
fs.writeFileSync(jsonFilePath, '{}');

// Read Excel file
const workbook = xlsx.readFile(excelFilePath);
const sheet = workbook.Sheets[workbook.SheetNames[0]];
const urls = [];
let row = 1; // Start reading from the first row

while (sheet[`A${row}`] && sheet[`A${row}`].v) {
    urls.push(sheet[`A${row}`].v);
    row++;
}

if (cluster.isMaster) {
    console.log(chalk.blue(`Master process ${process.pid} is running`));

    const taskStatus = urls.map((url, index) => ({
        id: index + 1,
        status: 'Pending',
        progress: 0,
        duration: '-'
    }));

    const workerStatus = {};
    const pdfInfo = {};
    let completedTasks = 0;
    let timedOutTasks = 0;
    let totalProcessedTasks = 0;

    // Task queue
    const taskQueue = urls.map((url, index) => ({ url, index }));
    const startTimes = {};
    const inProgress = new Set();

    function assignTask(workerId) {
        if (taskQueue.length > 0) {
            const task = taskQueue.shift();
            startTimes[task.index] = Date.now();
            inProgress.add(task.index);
            taskStatus[task.index].status = 'In Progress';
            taskStatus[task.index].progress = 10;
            cluster.workers[workerId].send({ type: 'start', url: task.url, index: task.index });

            // Set task timeout
            setTimeout(() => {
                if (inProgress.has(task.index)) {
                    console.log(chalk.red(`Task ${task.index + 1} timed out, skipping`));
                    inProgress.delete(task.index);
                    taskStatus[task.index].status = 'Timed Out';
                    taskStatus[task.index].progress = 0;
                    timedOutTasks++;
                    totalProcessedTasks++;
                    checkCompletion();
                    assignTask(workerId);
                }
            }, TASK_TIMEOUT);
        }
    }

    function updateConsole() {
        console.clear();
        console.log(chalk.blue(`Master process ${process.pid} is running`));
        console.log(chalk.yellow('Worker process status:'));
        Object.entries(workerStatus).forEach(([id, status]) => {
            console.log(chalk.cyan(`  Process ${id}: ${status}`));
        });
        console.log(chalk.yellow('Task status:'));
        taskStatus.forEach(task => {
            const statusColor = task.status === 'Pending' ? chalk.yellow :
                                task.status === 'In Progress' ? chalk.blue :
                                task.status === 'Completed' ? chalk.green :
                                task.status === 'Timed Out' ? chalk.red : chalk.white;
            console.log(`  Task ${task.id}: [${task.progress}%] ${statusColor(task.status)} (${task.duration})`);
        });
        console.log(chalk.yellow(`Total tasks: ${urls.length}, Completed: ${completedTasks}, Timed Out: ${timedOutTasks}, Remaining: ${urls.length - totalProcessedTasks}`));
    }

    // Periodically update console display
    const consoleUpdateInterval = setInterval(updateConsole, 1000);

    function checkCompletion() {
        if (totalProcessedTasks === urls.length) {
            clearInterval(consoleUpdateInterval);
            updateConsole(); // Final console update
            console.log(chalk.green('\nAll tasks completed'));
            console.log(chalk.green(`Total tasks: ${urls.length}`));
            console.log(chalk.green(`Successfully completed: ${completedTasks}`));
            console.log(chalk.red(`Timed out tasks: ${timedOutTasks}`));
            console.log(chalk.green('Shutting down all worker processes...'));
            // Close all worker processes
            for (const id in cluster.workers) {
                cluster.workers[id].kill();
            }
        }
    }

    // Initial task assignment
    for (let i = 0; i < numCPUs; i++) {
        const worker = cluster.fork();
        workerStatus[worker.id] = 'Idle';
        worker.on('online', () => {
            assignTask(worker.id);
        });
    }

    // Restart worker if it exits
    cluster.on('exit', (worker, code, signal) => {
        console.log(chalk.red(`Worker process ${worker.process.pid} has exited`));
        delete workerStatus[worker.id];
        if (taskQueue.length > 0 || inProgress.size > 0) {
            console.log(chalk.yellow('Starting a new worker process...'));
            const newWorker = cluster.fork();
            workerStatus[newWorker.id] = 'Idle';
            newWorker.on('online', () => {
                assignTask(newWorker.id);
            });
        }
    });
} else {
    process.send({ type: 'status', workerId: cluster.worker.id, status: 'Started' });

    async function processUrl(url, index) {
        process.send({ type: 'progress', index: index, progress: 10 });
        process.send({ type: 'status', workerId: cluster.worker.id, status: 'Starting browser' });
        const browser = await puppeteer.launch();
        const page = await browser.newPage();

        process.send({ type: 'status', workerId: cluster.worker.id, status: 'Loading page' });
        await page.goto(url, { waitUntil: 'networkidle2' });
        process.send({ type: 'progress', index: index, progress: 30 });

        process.send({ type: 'status', workerId: cluster.worker.id, status: 'Scrolling page' });
        await autoScroll(page);
        process.send({ type: 'progress', index: index, progress: 50 });

        process.send({ type: 'status', workerId: cluster.worker.id, status: 'Waiting for load to complete' });
        await new Promise(resolve => setTimeout(resolve, 10000));
        process.send({ type: 'progress', index: index, progress: 70 });

        await page.waitForFunction(() => document.readyState === "complete");

        process.send({ type: 'status', workerId: cluster.worker.id, status: 'Generating PDF' });
        
        // Generate new filename format
        const date = new Date();
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const day = String(date.getDate()).padStart(2, '0');
        const number = String(index + 1).padStart(2, '0');
        const fileName = `${month}${day}-${number}.pdf`;
        
        const filePath = path.join(outputDir, fileName);
        await page.pdf({
            path: filePath,
            format: 'A4',
            printBackground: true,
        });
        process.send({ type: 'progress', index: index, progress: 90 });

        await browser.close();

        process.send({ type: 'completed', index: index, workerId: cluster.worker.id, fileName: fileName });
    }

    process.on('message', async (msg) => {
        if (msg.type === 'start') {
            await processUrl(msg.url, msg.index);
        }
    });
}

async function autoScroll(page) {
    await page.evaluate(async () => {
        await new Promise((resolve, reject) => {
            let totalHeight = 0;
            const distance = 100;
            const timer = setInterval(() => {
                const scrollHeight = document.body.scrollHeight;
                window.scrollBy(0, distance);
                totalHeight += distance;

                if (totalHeight >= scrollHeight) {
                    clearInterval(timer);
                    resolve();
                }
            }, 200);
        });
    });
}

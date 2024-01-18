const SERVER_PORT = process.env.SERVER_PORT || 50051;
const VRFY_ENDPOINT = process.env.VRFY_ENDPOINT || "localhost";
const VRFY_PORT = process.env.VRFY_PORT || 9090;
const PUBSUB_PROJECT = process.env.PUBSUB_PROJECT || "jobrapido-sandbox";
const PUBSUB_TOPIC = process.env.PUBSUB_TOPIC || "hackathon-q1fy2024-brescianini";

const SERVICE_PROTO = __dirname + '/../protos/service.proto';
const VRFY_PROTO = __dirname + '/../protos/vrfy.proto';
const PUBSUB_PROTO = __dirname + '/../protos/pubsub.proto';

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const dns = require('dns');
const { PubSub } = require('@google-cloud/pubsub');
const protobuf = require('protobufjs');

const serviceDefinition = protoLoader.loadSync(
    SERVICE_PROTO,
    {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
    }
);
const service = grpc.loadPackageDefinition(serviceDefinition);

const vrfyDefinition = protoLoader.loadSync(
    VRFY_PROTO,
    {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
    }
);
const vrfy = grpc.loadPackageDefinition(vrfyDefinition);
const vrfyClient = new vrfy.VrfyService(`${VRFY_ENDPOINT}:${VRFY_PORT}`, grpc.credentials.createInsecure());

const VerificationOutput = protobuf.loadSync(PUBSUB_PROTO).lookupType("VerificationOutput");

const publisher = new PubSub({ projectId: PUBSUB_PROJECT });

const SYNTAX_REGEX = /(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])/;

const publishOptions = {
    gaxOpts: {
        timeout: 100 * 1000,
    },
};

let publishErrors = 0;

function publishMessage(request, message) {
    try {
        const verificationOutput = VerificationOutput.create({
            email: request.email,
            valid: message.valid,
            errorMessage: message.error_message
        });
        const dataBuffer = Buffer.from(VerificationOutput.encode(verificationOutput).finish());
        publisher.topic(PUBSUB_TOPIC, publishOptions).publishMessage({ data: dataBuffer }, (err, message_id) => {
            if (err) {
                if (++publishErrors % 100000 === 0) {
                    console.log(`Publish errors: ${publishErrors}, last error: ${err}`);
                }
            }
        });
    } catch (e) {
        console.log("Generic error");
    }

    return message;
}

function isSyntaxValid(email) {
    return SYNTAX_REGEX.test(email);
}

function performSyntaxVerification(request) {
    if (isSyntaxValid(request.email)) {
        return {
            valid: true,
        };
    }

    return {
        valid: false,
        error_message: 'Syntax error'
    };
}

function syntaxVerification(call, callback) {
    callback(null, publishMessage(call.request, performSyntaxVerification(call.request)));
}

const verifiedDomains = {};

async function isDomainValid(domain) {
    return new Promise((resolve, reject) => {
        if (verifiedDomains[domain] !== undefined) {
            resolve(verifiedDomains[domain]);
        } else {
            dns.resolveMx(domain, (err, addresses) => {
                if (err) {
                    verifiedDomains[domain] = false;
                    resolve(false);
                } else {
                    verifiedDomains[domain] = true;
                    resolve(true);
                }
            });
        }
    });
}

async function performSimpleVerification(request) {
    try {
        if (!isSyntaxValid(request.email)) {
            return {
                valid: false,
                error_message: 'Syntax error'
            };
        }

        const domain = request.email.split('@')[1];

        if (await isDomainValid(domain)) {
            return {
                valid: true,
            };
        } else {
            return {
                valid: false,
                error_message: 'Domain error'
            };
        }
    } catch (e) {
        return {
            valid: false,
            error_message: 'Simple verification error'
        };
    }
}

async function simpleVerification(call, callback) {
    callback(null, publishMessage(call.request, await performSimpleVerification(call.request)));
}

async function vrfyVerify(email) {
    return new Promise((resolve, reject) => {
        vrfyClient.verify({ email: email }, { deadline: new Date(Date.now() + 1000) }, (err, response) => {
            if (err) {
                resolve({
                    valid: false,
                    error_message: 'Vrfy verification error'
                });
            } else {
                if (response.status_code === 0) {
                    resolve({
                        valid: true,
                    });
                } else {
                    resolve({
                        valid: false,
                        error_message: `code:${response.status_code}|message:${response.error_message}`
                    });
                }
            }
        });
    });
}

async function performFullVerification(request) {
    try {
        if (!isSyntaxValid(request.email)) {
            return {
                valid: false,
                error_message: 'Syntax error'
            };
        }

        const domain = request.email.split('@')[1];

        if (!await isDomainValid(domain)) {
            return {
                valid: false,
                error_message: 'Domain error'
            };
        }

        return await vrfyVerify(request.email);
    } catch (e) {
        return {
            valid: false,
            error_message: 'Full verification error'
        };
    }
}

async function fullVerification(call, callback) {
    callback(null, publishMessage(call.request, await performFullVerification(call.request)));
}

function getServer() {
    var server = new grpc.Server();
    server.addService(service.MailVerifier.service, {
        syntaxVerification: syntaxVerification,
        simpleVerification: simpleVerification,
        fullVerification: fullVerification
    });
    return server;
}

if (require.main === module) {
    const server = getServer();
    process.on('SIGINT', () => {
        server.tryShutdown(() => {
            console.log('SIGNAL BREAK RECEIVED')
        });
    });
    server.bindAsync(`0.0.0.0:${SERVER_PORT}`, grpc.ServerCredentials.createInsecure(), () => {
        console.log(`Server listening on port ${SERVER_PORT}`);
        server.start();
    });
}
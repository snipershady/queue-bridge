<?php

declare(strict_types=1);

namespace QueueBridge\Tests\Unit\Transport\Redis;

use DateTimeImmutable;
use InvalidArgumentException;
use LogicException;
use Override;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\UsesClass;
use PHPUnit\Framework\TestCase;
use QueueBridge\Codec\EnvelopeCodec;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Exception\BatchSendException;
use QueueBridge\Exception\CommandFailedException;
use QueueBridge\Exception\SerializationException;
use QueueBridge\Message\Delivery;
use QueueBridge\Message\Envelope;
use QueueBridge\Message\FailureKind;
use QueueBridge\Message\FailureReason;
use QueueBridge\Message\Message;
use QueueBridge\Message\MessageId;
use QueueBridge\Message\MessageRules;
use QueueBridge\Tests\Support\FakeClock;
use QueueBridge\Tests\Support\RecordingLogger;
use QueueBridge\Tests\Support\RecordingRedisClient;
use QueueBridge\Transport\DeadLetter;
use QueueBridge\Transport\MaintenanceReport;
use QueueBridge\Transport\OrphanRecovery;
use QueueBridge\Transport\QueuedMessage;
use QueueBridge\Transport\QueuedMessageState;
use QueueBridge\Transport\QueueStats;
use QueueBridge\Transport\Redis\Feature\AckDelStrategy;
use QueueBridge\Transport\Redis\Feature\AckedTrimStrategy;
use QueueBridge\Transport\Redis\Feature\AckThenDeleteStrategy;
use QueueBridge\Transport\Redis\Feature\DelexLockRelease;
use QueueBridge\Transport\Redis\Feature\FeatureSelector;
use QueueBridge\Transport\Redis\Feature\GroupInfo;
use QueueBridge\Transport\Redis\Feature\GroupInfoLagStrategy;
use QueueBridge\Transport\Redis\Feature\LuaLockRelease;
use QueueBridge\Transport\Redis\Feature\MinIdTrimStrategy;
use QueueBridge\Transport\Redis\Feature\NoTrimStrategy;
use QueueBridge\Transport\Redis\Feature\NullLagStrategy;
use QueueBridge\Transport\Redis\Feature\PendingClaimStrategy;
use QueueBridge\Transport\Redis\Feature\PendingEntries;
use QueueBridge\Transport\Redis\Feature\PendingIdleStrategy;
use QueueBridge\Transport\Redis\Feature\ReadGroupClaimStrategy;
use QueueBridge\Transport\Redis\Feature\RedisFeatures;
use QueueBridge\Transport\Redis\RedisKeys;
use QueueBridge\Transport\Redis\RedisReply;
use QueueBridge\Transport\Redis\RedisTransport;
use QueueBridge\Transport\Redis\Script\RedisScripts;
use QueueBridge\Transport\Redis\Script\ScriptRunner;
use QueueBridge\Transport\Redis\Version\RedisCapabilities;
use QueueBridge\Transport\Redis\Version\RedisVersionEnum;
use QueueBridge\Transport\TransportCapabilities;

#[CoversClass(RedisTransport::class)]
#[UsesClass(RedisKeys::class)]
#[UsesClass(RedisReply::class)]
#[UsesClass(ScriptRunner::class)]
#[UsesClass(RedisScripts::class)]
#[UsesClass(RedisFeatures::class)]
#[UsesClass(FeatureSelector::class)]
#[UsesClass(RedisCapabilities::class)]
#[UsesClass(RedisVersionEnum::class)]
#[UsesClass(PendingClaimStrategy::class)]
#[UsesClass(PendingIdleStrategy::class)]
#[UsesClass(ReadGroupClaimStrategy::class)]
#[UsesClass(PendingEntries::class)]
#[UsesClass(AckThenDeleteStrategy::class)]
#[UsesClass(AckDelStrategy::class)]
#[UsesClass(NoTrimStrategy::class)]
#[UsesClass(MinIdTrimStrategy::class)]
#[UsesClass(AckedTrimStrategy::class)]
#[UsesClass(GroupInfo::class)]
#[UsesClass(NullLagStrategy::class)]
#[UsesClass(GroupInfoLagStrategy::class)]
#[UsesClass(LuaLockRelease::class)]
#[UsesClass(DelexLockRelease::class)]
#[UsesClass(EnvelopeCodec::class)]
#[UsesClass(QueueConfig::class)]
#[UsesClass(Message::class)]
#[UsesClass(MessageId::class)]
#[UsesClass(MessageRules::class)]
#[UsesClass(Delivery::class)]
#[UsesClass(Envelope::class)]
#[UsesClass(FailureReason::class)]
#[UsesClass(DeadLetter::class)]
#[UsesClass(MaintenanceReport::class)]
#[UsesClass(QueueStats::class)]
#[UsesClass(QueuedMessage::class)]
#[UsesClass(TransportCapabilities::class)]
#[UsesClass(CommandFailedException::class)]
#[UsesClass(BatchSendException::class)]
#[UsesClass(SerializationException::class)]
final class RedisTransportTest extends TestCase
{
    private const string STREAM = 'rq:{q}:stream';

    private const string DELAYED = 'rq:{q}:delayed';

    private const string DLQ = 'rq:{q}:dlq';

    private const string ID = '0190f2a4-5b6c-7d8e-9f01-23456789abcd';

    private FakeClock $fakeClock;

    private RecordingRedisClient $recordingRedisClient;

    private EnvelopeCodec $envelopeCodec;

    #[Override]
    protected function setUp(): void
    {
        $this->fakeClock = new FakeClock('2026-01-01T00:00:00.000Z');
        $this->recordingRedisClient = new RecordingRedisClient(static fn (array $command): ?int => match ($command[0]) {
            'EVALSHA' => 0,
            'XLEN', 'ZCARD' => 0,
            default => null,
        });
        $this->envelopeCodec = new EnvelopeCodec();
    }

    // --- invio ----------------------------------------------------------------------------------------------

    public function testSendAddsToTheStream(): void
    {
        $message = $this->message();

        $this->transport()->send($message);

        self::assertSame([['XADD', self::STREAM, '*', ...$this->flat($this->envelopeCodec->encode($message))]], $this->recordingRedisClient->commands);
    }

    public function testSendWithDelayAddsToTheSortedSet(): void
    {
        $message = $this->message();

        $this->transport()->send($message, 1500);

        $member = json_encode($this->envelopeCodec->encode($message), \JSON_THROW_ON_ERROR | \JSON_UNESCAPED_UNICODE | \JSON_UNESCAPED_SLASHES);
        self::assertSame([['ZADD', self::DELAYED, '1767225601500', $member]], $this->recordingRedisClient->commands);
    }

    public function testDelayedMembersKeepUnicodeUnescaped(): void
    {
        $message = new Message(MessageId::fromString(self::ID), 'test', 'è/✓', new DateTimeImmutable('2026-01-01T00:00:00Z'));

        $this->transport()->send($message, 10);

        self::assertStringContainsString('"text":"è/✓"', $this->recordingRedisClient->commands[0][3]);
    }

    public function testDefaultArguments(): void
    {
        $redisTransport = $this->transport();
        $this->recordingRedisClient->reply('XRANGE', [])->reply('EVALSHA', 0)->reply('EVALSHA', 0);

        $redisTransport->receive();
        $redisTransport->deadLetters();
        $redisTransport->requeueDeadLetters();

        self::assertSame(['XREADGROUP', 'GROUP', 'queue-bridge', 'consumer-1', 'COUNT', '1', 'BLOCK', '1000', 'STREAMS', self::STREAM, '>'], $this->recordingRedisClient->named('XREADGROUP')[0]);
        self::assertSame(['XRANGE', self::DLQ, '-', '+', 'COUNT', '100'], $this->recordingRedisClient->named('XRANGE')[0]);
        self::assertSame(['first', '100'], \array_slice($this->recordingRedisClient->commands[3], -2));
    }

    public function testDeadLettersAcceptsTheMinimumLimit(): void
    {
        $this->recordingRedisClient->reply('XRANGE', []);

        self::assertSame([], $this->transport()->deadLetters(1));
    }

    public function testSendRejectsNegativeDelay(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->transport()->send($this->message(), -1);
    }

    public function testSendBatchUsesOnePipeline(): void
    {
        $this->recordingRedisClient->reply('XADD', '1-0')->reply('ZADD', 1);

        $this->transport()->sendBatch([new Envelope($this->message()), new Envelope($this->message('b'), 10)]);

        self::assertCount(1, $this->recordingRedisClient->pipelines);
        self::assertSame(['XADD', 'ZADD'], array_map(static fn (array $command): string => $command[0], $this->recordingRedisClient->pipelines[0]));
    }

    public function testEmptyBatchSendsNothing(): void
    {
        $this->transport()->sendBatch([]);

        self::assertSame([], $this->recordingRedisClient->pipelines);
    }

    public function testSendBatchReportsPartialFailures(): void
    {
        $error = CommandFailedException::rejected('XADD', 'OOM');
        $this->recordingRedisClient->reply('XADD', '1-0')->reply('XADD', $error)->reply('XADD', CommandFailedException::rejected('XADD', 'OOM again'));
        $first = $this->message('a');
        $second = $this->message('b');
        $third = $this->message('c');

        try {
            $this->transport()->sendBatch([new Envelope($first), new Envelope($second), new Envelope($third)]);
            self::fail('Expected BatchSendException');
        } catch (BatchSendException $batchSendException) {
            self::assertSame([$first->id], $batchSendException->sentIds);
            self::assertSame([$second->id, $third->id], $batchSendException->failedIds);
            self::assertSame($error, $batchSendException->getPrevious());
        }
    }

    // --- ricezione ------------------------------------------------------------------------------------------

    public function testReceivePromotesDelayedThenReadsNewEntries(): void
    {
        $message = $this->message();
        $this->recordingRedisClient->reply('XREADGROUP', [[self::STREAM, [['5-0', $this->flat($this->envelopeCodec->encode($message))]]]]);

        $deliveries = $this->transport()->receive(10, 2000);

        self::assertCount(1, $deliveries);
        self::assertSame('5-0', $deliveries[0]->receipt);
        self::assertSame(1, $deliveries[0]->deliveryCount);
        self::assertTrue($deliveries[0]->message->id->equals($message->id));
        self::assertSame(['EVALSHA', sha1(RedisScripts::promoteDelayed()), '2', self::DELAYED, self::STREAM, '1767225600000', '100'], $this->recordingRedisClient->commands[0]);
        self::assertSame(['XREADGROUP', 'GROUP', 'queue-bridge', 'consumer-1', 'COUNT', '10', 'BLOCK', '2000', 'STREAMS', self::STREAM, '>'], $this->recordingRedisClient->commands[1]);
    }

    public function testReceiveWithZeroTimeoutDoesNotBlock(): void
    {
        self::assertSame([], $this->transport()->receive(1, 0));
        self::assertSame(['XREADGROUP', 'GROUP', 'queue-bridge', 'consumer-1', 'COUNT', '1', 'STREAMS', self::STREAM, '>'], $this->recordingRedisClient->commands[1]);
    }

    public function testReceiveAddsDeliveriesFromPreviousCopies(): void
    {
        $fields = [...$this->envelopeCodec->encode($this->message()), RedisTransport::FIELD_DELIVERIES => '3'];
        $this->recordingRedisClient->reply('XREADGROUP', [[self::STREAM, [['5-0', $this->flat($fields)]]]]);

        self::assertSame(4, $this->transport()->receive()[0]->deliveryCount);
    }

    public function testReceiveClaimsOrphansOnRedis84(): void
    {
        $fields = [...$this->envelopeCodec->encode($this->message()), RedisTransport::FIELD_DELIVERIES => '1'];
        $this->recordingRedisClient->reply('XREADGROUP', [[self::STREAM, [
            ['5-0', $this->flat($fields), 1500, 2],
            ['6-0', $this->flat($this->envelopeCodec->encode($this->message('new')))],
        ]]]);

        $deliveries = $this->transport(RedisVersionEnum::GE84)->receive(5, 0);

        self::assertSame([4, 1], [$deliveries[0]->deliveryCount, $deliveries[1]->deliveryCount]);
        self::assertSame(['XREADGROUP', 'GROUP', 'queue-bridge', 'consumer-1', 'COUNT', '5', 'CLAIM', '10000', 'STREAMS', self::STREAM, '>'], $this->recordingRedisClient->commands[1]);
    }

    public function testReceiveCreatesTheGroupWhenMissing(): void
    {
        $this->recordingRedisClient
            ->reply('XREADGROUP', CommandFailedException::rejected('XREADGROUP', "NOGROUP No such key 'rq:{q}:stream' or consumer group"))
            ->reply('XGROUP', 'OK')
            ->reply('XREADGROUP', reply: null);

        self::assertSame([], $this->transport()->receive(1, 0));
        self::assertSame(['EVALSHA', 'XREADGROUP', 'XGROUP', 'XREADGROUP'], $this->recordingRedisClient->names());
    }

    public function testReceiveRethrowsOtherErrors(): void
    {
        $this->recordingRedisClient->reply('XREADGROUP', CommandFailedException::rejected('XREADGROUP', 'NOPERM'));

        $this->expectException(CommandFailedException::class);
        $this->expectExceptionMessage('NOPERM');

        $this->transport()->receive();
    }

    public function testReceiveAcknowledgesEntriesDeletedWhilePending(): void
    {
        $this->recordingRedisClient->reply('XREADGROUP', [[self::STREAM, [['5-0', null, 2000, 1]]]]);

        self::assertSame([], $this->transport()->receive());
        self::assertSame(['EVALSHA', sha1(RedisScripts::ack()), '1', self::STREAM, 'queue-bridge', 'consumer-1', 'xack_xdel', '5-0'], $this->recordingRedisClient->commands[2]);
    }

    public function testReceiveMovesUndecodableEntriesToTheDeadLetterQueue(): void
    {
        $recordingLogger = new RecordingLogger();
        $this->recordingRedisClient->reply('XREADGROUP', [[self::STREAM, [['5-0', ['v', '9', 'id', self::ID]]]]])->reply('EVALSHA', 0)->reply('EVALSHA', 1);

        self::assertSame([], $this->transport(logger: $recordingLogger)->receive());

        $settle = $this->recordingRedisClient->commands[2];
        self::assertSame(['EVALSHA', sha1(RedisScripts::settle()), '3', self::STREAM, self::DELAYED, self::DLQ, 'queue-bridge', 'consumer-1', 'xack_xdel', '5-0', 'dlq', '100000', ''], \array_slice($settle, 0, 13));
        self::assertSame(
            ['v', '9', 'id', self::ID, 'failure_kind', 'serialization', 'failure_error_class', SerializationException::class, 'failure_message', 'Unsupported wire format version "9".', 'failure_at', '1767225600000', 'failure_original_id', self::ID],
            \array_slice($settle, 13),
        );
        self::assertSame(['error'], $recordingLogger->levels());
    }

    public function testUndecodableEntryWithoutIdKeepsAnEmptyOriginalId(): void
    {
        $this->recordingRedisClient->reply('XREADGROUP', [[self::STREAM, [['5-0', ['garbage', 'x']]]]]);

        $this->transport()->receive();

        self::assertSame(['failure_original_id', ''], \array_slice($this->recordingRedisClient->commands[2], -2));
    }

    public function testReceiveRejectsInvalidArguments(): void
    {
        foreach ([[0, 0], [1, -1]] as [$max, $timeout]) {
            try {
                $this->transport()->receive($max, $timeout);
                self::fail('Expected InvalidArgumentException');
            } catch (InvalidArgumentException $invalidArgumentException) {
                self::assertStringContainsString('must be >= ', $invalidArgumentException->getMessage());
            }
        }
    }

    // --- conferma, retry, reject, release ------------------------------------------------------------------------

    public function testAckBatchConfirmsAllReceiptsInOneScript(): void
    {
        $this->transport(RedisVersionEnum::GE82)->ackBatch([$this->delivery('1-0'), $this->delivery('2-0')]);
        $this->transport()->ack($this->delivery('3-0'));
        $this->transport()->ackBatch([]);

        self::assertSame(['EVALSHA', sha1(RedisScripts::ack()), '1', self::STREAM, 'queue-bridge', 'consumer-1', 'xackdel', '1-0', '2-0'], $this->recordingRedisClient->commands[0]);
        self::assertSame(['EVALSHA', sha1(RedisScripts::ack()), '1', self::STREAM, 'queue-bridge', 'consumer-1', 'xack_xdel', '3-0'], $this->recordingRedisClient->commands[1]);
        self::assertCount(2, $this->recordingRedisClient->commands);
    }

    public function testDeliveriesNoLongerOwnedAreIgnoredWithADebugLog(): void
    {
        $recordingLogger = new RecordingLogger();
        $redisTransport = $this->transport(logger: $recordingLogger);
        $this->recordingRedisClient->reply('EVALSHA', 2)->reply('EVALSHA', 1)->reply('EVALSHA', 1);

        $redisTransport->ackBatch([$this->delivery('1-0'), $this->delivery('2-0')]);
        $redisTransport->ack($this->delivery('3-0'));
        $redisTransport->release($this->delivery('4-0'));
        self::assertSame([], $recordingLogger->levels(), 'owned deliveries: nothing to report');

        $redisTransport->ackBatch([$this->delivery('5-0'), $this->delivery('6-0')]);
        $redisTransport->retry($this->delivery('7-0'), 0);

        self::assertSame(['debug', 'debug'], $recordingLogger->levels());
        self::assertSame(['count' => 2], $recordingLogger->records[0]['context']);
        self::assertSame(['target' => 'stream', 'id' => '7-0'], $recordingLogger->records[1]['context']);
    }

    public function testRetryNowAddsTheNextAttemptToTheStream(): void
    {
        $delivery = $this->delivery('7-0');

        $this->transport()->retry($delivery, 0);

        $fields = $this->envelopeCodec->encode($delivery->message->withIncrementedAttempts());
        self::assertSame(
            ['EVALSHA', sha1(RedisScripts::settle()), '3', self::STREAM, self::DELAYED, self::DLQ, 'queue-bridge', 'consumer-1', 'xack_xdel', '7-0', 'stream', '0', '', ...$this->flat($fields)],
            $this->recordingRedisClient->commands[0],
        );
    }

    public function testRetryLaterSchedulesTheNextAttempt(): void
    {
        $delivery = $this->delivery('7-0');

        $this->transport()->retry($delivery, 2500);

        $member = json_encode($this->envelopeCodec->encode($delivery->message->withIncrementedAttempts()), \JSON_THROW_ON_ERROR | \JSON_UNESCAPED_UNICODE | \JSON_UNESCAPED_SLASHES);
        self::assertSame(['7-0', 'delayed', '1767225602500', $member], \array_slice($this->recordingRedisClient->commands[0], 9));
        self::assertCount(1, $this->recordingRedisClient->commands);
    }

    public function testRetryRejectsNegativeDelay(): void
    {
        $this->expectException(InvalidArgumentException::class);

        $this->transport()->retry($this->delivery('7-0'), -1);
    }

    public function testRejectWritesTheMessageAndTheReasonToTheDeadLetterQueue(): void
    {
        $delivery = $this->delivery('7-0');
        $failureReason = FailureReason::fromThrowable(FailureKind::HandlerFailed, $delivery->message->id, $this->fakeClock->now(), new LogicException('boom'));

        $this->transport(queueConfig: new QueueConfig(name: 'q', dlqMaxLength: 50))->reject($delivery, $failureReason);

        self::assertSame(
            ['7-0', 'dlq', '50', '', ...$this->flat($this->envelopeCodec->encode($delivery->message)),
                'failure_kind', 'handler_failed', 'failure_error_class', LogicException::class, 'failure_message', 'boom', 'failure_at', '1767225600000', 'failure_original_id', self::ID],
            \array_slice($this->recordingRedisClient->commands[0], 9),
        );
    }

    public function testRejectWithoutExceptionStoresAnEmptyErrorClass(): void
    {
        $delivery = $this->delivery('7-0');

        $this->transport()->reject($delivery, new FailureReason(FailureKind::Expired, $delivery->message->id, $this->fakeClock->now()));

        self::assertContains('failure_error_class', $this->recordingRedisClient->commands[0]);
        $index = array_search('failure_error_class', $this->recordingRedisClient->commands[0], strict: true);
        self::assertIsInt($index);
        self::assertSame('', $this->recordingRedisClient->commands[0][$index + 1]);
    }

    public function testReleaseRequeuesKeepingTheDeliveryCount(): void
    {
        $delivery = $this->delivery('7-0', 2);

        $this->transport()->release($delivery);

        self::assertSame(
            ['7-0', 'stream', '0', '', ...$this->flat([...$this->envelopeCodec->encode($delivery->message), 'deliveries' => '2'])],
            \array_slice($this->recordingRedisClient->commands[0], 9),
        );
    }

    // --- setup, ping, capabilities -------------------------------------------------------------------------------

    public function testSetupCreatesTheGroupFromTheBeginningOfTheStream(): void
    {
        $this->recordingRedisClient->reply('XGROUP', 'OK')->reply('XGROUP', CommandFailedException::rejected('XGROUP', 'BUSYGROUP Consumer Group name already exists'));

        $this->transport()->setup();
        $this->transport()->setup();

        self::assertSame([['XGROUP', 'CREATE', self::STREAM, 'queue-bridge', '0', 'MKSTREAM']], array_values(array_unique($this->recordingRedisClient->commands, \SORT_REGULAR)));
    }

    public function testSetupRethrowsOtherErrors(): void
    {
        $this->recordingRedisClient->reply('XGROUP', CommandFailedException::rejected('XGROUP', 'NOPERM'));

        $this->expectException(CommandFailedException::class);

        $this->transport()->setup();
    }

    public function testPing(): void
    {
        $this->transport()->ping();

        self::assertSame([['PING']], $this->recordingRedisClient->commands);
    }

    public function testCapabilitiesDependOnTheLevel(): void
    {
        $old = $this->transport(RedisVersionEnum::GE62)->capabilities();
        $new = $this->transport(RedisVersionEnum::GE70)->capabilities();

        self::assertSame(OrphanRecovery::Explicit, $old->orphanRecovery);
        self::assertTrue($old->nativeDelay && $old->batchReceive && $old->batchAck && $old->deadLetter && $old->deliveryCount);
        self::assertFalse($old->consumerLag);
        self::assertTrue($new->consumerLag);
    }

    public function testConsumerNameIsRequired(): void
    {
        $this->expectException(InvalidArgumentException::class);

        new RedisTransport($this->recordingRedisClient, FeatureSelector::select(RedisCapabilities::forVersion(RedisVersionEnum::GE60)), ' ', new QueueConfig(name: 'q'), $this->fakeClock);
    }

    // --- manutenzione ---------------------------------------------------------------------------------------------

    public function testMaintainPromotesRequeuesOrphansAndTrims(): void
    {
        $this->recordingRedisClient
            ->reply('EVALSHA', 100)
            ->reply('EVALSHA', 3)
            ->reply('XPENDING', [['1-0', 'c', 20_000, 1]])
            ->reply('EVALSHA', 1)
            ->reply('XINFO', [['name', 'queue-bridge', 'last-delivered-id', '0-0']]);

        $maintenanceReport = $this->transport(RedisVersionEnum::GE62)->maintain();

        self::assertSame([103, 1, 0], [$maintenanceReport->promotedDelayed, $maintenanceReport->recoveredOrphans, $maintenanceReport->trimmed]);
        self::assertSame(['EVALSHA', 'EVALSHA', 'XPENDING', 'EVALSHA', 'XINFO'], $this->recordingRedisClient->names());
        self::assertSame(['EVALSHA', sha1(RedisScripts::requeueOrphans()), '1', self::STREAM, 'queue-bridge', 'xack_xdel', '10000', '1-0'], $this->recordingRedisClient->commands[3]);
    }

    public function testMaintainScansOrphansPageByPage(): void
    {
        $page = array_map(static fn (int $i): array => [$i.'-0', 'c', 20_000, 1], range(1, 100));
        $this->recordingRedisClient
            ->reply('XPENDING', $page)
            ->reply('EVALSHA', 0)
            ->reply('EVALSHA', 100)
            ->reply('XPENDING', [])
            ->reply('EVALSHA', 0);

        $maintenanceReport = $this->transport()->maintain();

        self::assertSame(100, $maintenanceReport->recoveredOrphans);
        self::assertCount(2, $this->recordingRedisClient->named('XPENDING'));
        $requeue = $this->recordingRedisClient->commands[2];
        self::assertSame(sha1(RedisScripts::requeueOrphans()), $requeue[1]);
        self::assertCount(7 + 100, $requeue);
        self::assertSame(['1-0', '100-0'], [$requeue[7], $requeue[106]]);
    }

    public function testMaintainStopsAfterTheMaximumNumberOfPages(): void
    {
        $page = array_map(static fn (int $i): array => [$i.'-0', 'c', 20_000, 1], range(1, 100));
        $this->recordingRedisClient = new RecordingRedisClient(static fn (array $command): array|int|null => match ($command[0]) {
            'XPENDING' => $page,
            'EVALSHA' => \in_array(sha1(RedisScripts::requeueOrphans()), $command, strict: true) ? 100 : 0,
            default => null,
        });

        self::assertSame(1000, $this->transport()->maintain()->recoveredOrphans);
        self::assertCount(10, $this->recordingRedisClient->named('XPENDING'));
    }

    public function testMaintainOnRedis84LeavesOrphansToTheReader(): void
    {
        $this->recordingRedisClient->reply('XTRIM', 2);

        $maintenanceReport = $this->transport(RedisVersionEnum::GE84)->maintain();

        self::assertSame([0, 0, 2], [$maintenanceReport->promotedDelayed, $maintenanceReport->recoveredOrphans, $maintenanceReport->trimmed]);
        self::assertSame(['EVALSHA', 'XTRIM'], $this->recordingRedisClient->names());
    }

    // --- statistiche ----------------------------------------------------------------------------------------------

    public function testStatsCombineStreamGroupAndDelayed(): void
    {
        $this->recordingRedisClient
            ->reply('XINFO', [['name', 'queue-bridge', 'consumers', 2, 'pending', 3, 'last-delivered-id', '9-0', 'lag', 4]])
            ->reply('XLEN', 10)
            ->reply('ZCARD', 5)
            ->reply('XLEN', 1);

        $queueStats = $this->transport(RedisVersionEnum::GE70)->stats();

        self::assertSame([7, 3, 5, 1, 4, 2], [$queueStats->ready, $queueStats->inFlight, $queueStats->delayed, $queueStats->deadLetters, $queueStats->lag, $queueStats->consumers]);
    }

    public function testReadyIsNeverNegative(): void
    {
        $this->recordingRedisClient
            ->reply('XINFO', [['name', 'queue-bridge', 'consumers', 1, 'pending', 3]])
            ->reply('XLEN', 2);

        self::assertSame(0, $this->transport()->stats()->ready);
    }

    public function testStatsBeforeSetup(): void
    {
        $this->recordingRedisClient->reply('XINFO', CommandFailedException::rejected('XINFO', 'ERR no such key'));

        $queueStats = $this->transport()->stats();

        self::assertSame([0, 0, 0, 0, null, null], [$queueStats->ready, $queueStats->inFlight, $queueStats->delayed, $queueStats->deadLetters, $queueStats->lag, $queueStats->consumers]);
    }

    // --- dead letter ----------------------------------------------------------------------------------------------

    public function testDeadLettersDecodeMessageAndReason(): void
    {
        $message = $this->message();
        $this->recordingRedisClient->reply('XRANGE', [
            ['1-0', $this->flat([...$this->envelopeCodec->encode($message), 'failure_kind' => 'unrecoverable', 'failure_error_class' => LogicException::class, 'failure_message' => 'bad', 'failure_at' => '1767225600123', 'failure_original_id' => self::ID])],
            ['2-0', ['v', '1', 'id', 'corrupt']],
            ['3-0', $this->flat([...$this->envelopeCodec->encode($message), 'failure_kind' => 'mystery', 'failure_error_class' => ''])],
        ]);

        $deadLetters = $this->transport()->deadLetters(3);

        self::assertSame(['XRANGE', self::DLQ, '-', '+', 'COUNT', '3'], $this->recordingRedisClient->commands[0]);
        self::assertCount(2, $deadLetters);
        self::assertSame(FailureKind::Unrecoverable, $deadLetters[0]->reason->kind);
        self::assertSame(LogicException::class, $deadLetters[0]->reason->errorClass);
        self::assertSame('bad', $deadLetters[0]->reason->errorMessage);
        self::assertSame('2026-01-01T00:00:00.123+00:00', $deadLetters[0]->reason->failedAt->format('Y-m-d\TH:i:s.vP'));
        self::assertTrue($deadLetters[0]->message->id->equals($message->id));
        self::assertSame(FailureKind::HandlerFailed, $deadLetters[1]->reason->kind);
        self::assertNull($deadLetters[1]->reason->errorClass);
        self::assertSame('', $deadLetters[1]->reason->errorMessage);
        self::assertSame('1970-01-01T00:00:00+00:00', $deadLetters[1]->reason->failedAt->format(\DATE_ATOM));
    }

    public function testRequeueDeadLetters(): void
    {
        $this->recordingRedisClient->reply('EVALSHA', 2);

        self::assertSame(2, $this->transport()->requeueDeadLetters(5));
        self::assertSame(['EVALSHA', sha1(RedisScripts::requeueDeadLetters()), '2', self::DLQ, self::STREAM, 'first', '5'], $this->recordingRedisClient->commands[0]);
    }

    public function testRequeueDeadLetterFindsTheEntryAcrossPages(): void
    {
        $other = $this->flat($this->envelopeCodec->encode($this->message('other')));
        $page = array_map(static fn (int $i): array => ['1-'.$i, $other], range(0, 499));
        $target = $this->message('target');
        $this->recordingRedisClient
            ->reply('XRANGE', $page)
            ->reply('XRANGE', [['2-0', $this->flat($this->envelopeCodec->encode($target))]])
            ->reply('EVALSHA', 1);

        self::assertTrue($this->transport()->requeueDeadLetter($target->id));

        self::assertSame(['XRANGE', self::DLQ, '-', '+', 'COUNT', '500'], $this->recordingRedisClient->commands[0]);
        self::assertSame(['XRANGE', self::DLQ, '1-500', '+', 'COUNT', '500'], $this->recordingRedisClient->commands[1]);
        self::assertSame(['EVALSHA', sha1(RedisScripts::requeueDeadLetters()), '2', self::DLQ, self::STREAM, 'id', '2-0'], $this->recordingRedisClient->commands[2]);
    }

    public function testRequeueDeadLetterReportsAMissingMessage(): void
    {
        $this->recordingRedisClient->reply('XRANGE', [['1', $this->flat($this->envelopeCodec->encode($this->message()))]]);

        self::assertFalse($this->transport()->requeueDeadLetter(MessageId::fromString('0190f2a4-5b6c-7d8e-9f01-000000000000')));
        self::assertSame(['XRANGE'], $this->recordingRedisClient->names());
    }

    public function testPurgeDeadLetters(): void
    {
        $this->recordingRedisClient->reply('EVALSHA', 4);

        self::assertSame(4, $this->transport()->purgeDeadLetters());
        self::assertSame(['EVALSHA', sha1(RedisScripts::purgeDeadLetters()), '1', self::DLQ], $this->recordingRedisClient->commands[0]);
    }

    public function testDeadLetterLimitsMustBePositive(): void
    {
        foreach ([fn (): array => $this->transport()->deadLetters(0), fn (): int => $this->transport()->requeueDeadLetters(0), fn (): array => $this->transport()->browse(0)] as $call) {
            try {
                $call();
                self::fail('Expected InvalidArgumentException');
            } catch (InvalidArgumentException $invalidArgumentException) {
                self::assertSame('Limit must be >= 1, got 0.', $invalidArgumentException->getMessage());
            }
        }
    }

    // --- svuotamento ed elenco ----------------------------------------------------------------------------------

    public function testPurgeClearsDelayedThenTheStreamPageByPageUpToTheNewestEntry(): void
    {
        $this->recordingRedisClient
            ->reply('EVALSHA', 2)
            ->reply('XREVRANGE', [['9-0', ['v', '1']]])
            ->reply('EVALSHA', [498, 500, '5-0'])
            ->reply('EVALSHA', [3, 10, '9-0']);

        self::assertSame(503, $this->transport()->purge());

        $purgeReady = sha1(RedisScripts::purgeReady());
        self::assertSame([
            ['EVALSHA', sha1(RedisScripts::purgeDelayed()), '1', self::DELAYED],
            ['XREVRANGE', self::STREAM, '+', '-', 'COUNT', '1'],
            ['EVALSHA', $purgeReady, '1', self::STREAM, 'queue-bridge', '-', '9-0', '500'],
            ['EVALSHA', $purgeReady, '1', self::STREAM, 'queue-bridge', '5-1', '9-0', '500'],
        ], $this->recordingRedisClient->commands);
    }

    public function testPurgeOfAnEmptyStreamOnlyClearsDelayed(): void
    {
        $this->recordingRedisClient->reply('EVALSHA', 4)->reply('XREVRANGE', []);

        self::assertSame(4, $this->transport()->purge());
        self::assertSame(['EVALSHA', 'XREVRANGE'], $this->recordingRedisClient->names());
    }

    public function testPurgeStopsWhenAPageIsEmpty(): void
    {
        $this->recordingRedisClient->reply('EVALSHA', 0)->reply('XREVRANGE', [['1-0', []]])->reply('EVALSHA', [0, 0, '']);

        self::assertSame(0, $this->transport()->purge());
        self::assertSame(['EVALSHA', 'XREVRANGE', 'EVALSHA'], $this->recordingRedisClient->names());
    }

    public function testBrowseMarksPendingEntriesAndAppendsDelayedMessages(): void
    {
        $inFlight = $this->message('flight');
        $ready = $this->message('ready');
        $delayed = $this->message('later');
        $this->recordingRedisClient
            ->reply('XRANGE', [
                ['1-0', $this->flat($this->envelopeCodec->encode($inFlight))],
                ['2-0', $this->flat($this->envelopeCodec->encode($ready))],
                ['3-0', ['v', '1', 'id', 'corrupt']],
            ])
            ->reply('XPENDING', [['1-0', 'consumer-1', '10', '1']])
            ->reply('ZRANGE', [
                'not json', '1',
                '5', '2',
                '{"a":1}', '3',
                '{"a":"b"}', '4',
                json_encode($this->envelopeCodec->encode($delayed), \JSON_THROW_ON_ERROR), '1767225660123',
            ]);

        $queued = $this->transport()->browse(10);

        self::assertSame([
            ['XRANGE', self::STREAM, '-', '+', 'COUNT', '10'],
            ['XPENDING', self::STREAM, 'queue-bridge', '1-0', '3-0', '3'],
            ['ZRANGE', self::DELAYED, '0', '6', 'WITHSCORES'],
        ], $this->recordingRedisClient->commands);
        self::assertSame(['flight', 'ready', 'later'], array_map(static fn (QueuedMessage $queuedMessage): string => $queuedMessage->message->text, $queued));
        self::assertSame(
            [QueuedMessageState::InFlight, QueuedMessageState::Ready, QueuedMessageState::Delayed],
            array_map(static fn (QueuedMessage $queuedMessage): QueuedMessageState => $queuedMessage->state, $queued),
        );
        self::assertSame('2026-01-01T00:01:00.123+00:00', $queued[2]->availableAt?->format('Y-m-d\TH:i:s.vP'));
    }

    public function testBrowseDoesNotReadDelayedWhenTheStreamFillsTheLimit(): void
    {
        $this->recordingRedisClient
            ->reply('XRANGE', [['1-0', $this->flat($this->envelopeCodec->encode($this->message('a')))], ['2-0', $this->flat($this->envelopeCodec->encode($this->message('b')))]])
            ->reply('XPENDING', []);

        self::assertCount(2, $this->transport()->browse(2));
        self::assertSame(['XRANGE', 'XPENDING'], $this->recordingRedisClient->names());
    }

    public function testBrowseOfAnEmptyQueue(): void
    {
        $this->recordingRedisClient->reply('XRANGE', [])->reply('ZRANGE', []);

        self::assertSame([], $this->transport()->browse());
        self::assertSame([['XRANGE', self::STREAM, '-', '+', 'COUNT', '100'], ['ZRANGE', self::DELAYED, '0', '99', 'WITHSCORES']], $this->recordingRedisClient->commands);
    }

    public function testBrowseBeforeTheGroupExistsSeesOnlyReadyMessages(): void
    {
        $this->recordingRedisClient
            ->reply('XRANGE', [['1-0', $this->flat($this->envelopeCodec->encode($this->message()))]])
            ->reply('XPENDING', CommandFailedException::rejected('XPENDING', 'NOGROUP No such key or consumer group'))
            ->reply('ZRANGE', []);

        $queued = $this->transport()->browse();

        self::assertCount(1, $queued);
        self::assertSame(QueuedMessageState::Ready, $queued[0]->state);
    }

    public function testBrowseRethrowsOtherErrors(): void
    {
        $this->recordingRedisClient
            ->reply('XRANGE', [['1-0', $this->flat($this->envelopeCodec->encode($this->message()))]])
            ->reply('XPENDING', CommandFailedException::rejected('XPENDING', 'ERR boom'));

        $this->expectException(CommandFailedException::class);
        $this->expectExceptionMessage('boom');

        $this->transport()->browse();
    }

    private function transport(
        RedisVersionEnum $redisVersionEnum = RedisVersionEnum::GE60,
        ?QueueConfig $queueConfig = null,
        ?RecordingLogger $logger = null,
    ): RedisTransport {
        return new RedisTransport(
            $this->recordingRedisClient,
            FeatureSelector::select(RedisCapabilities::forVersion($redisVersionEnum)),
            'consumer-1',
            $queueConfig ?? new QueueConfig(name: 'q', claimIdleMs: 10_000),
            $this->fakeClock,
            logger: $logger ?? new RecordingLogger(),
        );
    }

    private function message(string $text = 'x'): Message
    {
        $id = 'x' === $text ? self::ID : '0190f2a4-5b6c-7d8e-9f01-'.str_pad(bin2hex(\substr($text, 0, 6)), 12, '0', \STR_PAD_LEFT);

        return new Message(MessageId::fromString($id), 'test', $text, new DateTimeImmutable('2026-01-01T00:00:00Z'));
    }

    private function delivery(string $receipt, int $deliveryCount = 1): Delivery
    {
        return new Delivery($this->message(), $receipt, $this->fakeClock->now(), $deliveryCount);
    }

    /**
     * @param array<string, string> $fields
     *
     * @return list<string>
     */
    private function flat(array $fields): array
    {
        $flat = [];
        foreach ($fields as $name => $value) {
            $flat[] = $name;
            $flat[] = $value;
        }

        return $flat;
    }
}

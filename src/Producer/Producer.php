<?php

declare(strict_types=1);

namespace QueueBridge\Producer;

use InvalidArgumentException;
use Override;
use Psr\Clock\ClockInterface;
use QueueBridge\Config\QueueConfig;
use QueueBridge\Message\Envelope;
use QueueBridge\Message\Message;
use QueueBridge\Message\MessageId;
use QueueBridge\Message\OutgoingMessage;
use QueueBridge\System\IdGeneratorInterface;
use QueueBridge\System\SystemClock;
use QueueBridge\System\UuidV7Generator;
use QueueBridge\Transport\TransportInterface;

/**
 * Crea i messaggi (ID, data di creazione, scadenza), applica i middleware e i limiti di QueueConfig,
 * poi li consegna al transport.
 *
 * @api
 */
final readonly class Producer implements ProducerInterface
{
    private IdGeneratorInterface $idGenerator;

    /**
     * Complessità: O(1).
     *
     * @param list<ProducerMiddlewareInterface> $middlewares eseguiti nell'ordine dato
     */
    public function __construct(
        private TransportInterface $transport,
        private QueueConfig $config = new QueueConfig(),
        private ClockInterface $clock = new SystemClock(),
        ?IdGeneratorInterface $idGenerator = null,
        private array $middlewares = [],
    ) {
        $this->idGenerator = $idGenerator ?? new UuidV7Generator($clock);
    }

    /**
     * Complessità: O(m·(t + h)) più un invio al transport, t = byte del testo, h = byte degli header, m = middleware
     * del producer.
     */
    #[Override]
    public function publish(string $channel, string $text, array $headers = [], int $delayMs = 0, ?int $ttlMs = null): MessageId
    {
        $outgoingMessage = new OutgoingMessage($channel, $text, $headers, $delayMs, $ttlMs);
        [$id, $envelope] = $this->prepare($outgoingMessage);

        if ($envelope instanceof Envelope) {
            $this->transport->send($envelope->message, $envelope->delayMs);
        }

        return $id;
    }

    /**
     * Complessità: O(n·m·(t + h)) più un invio in batch al transport (un round trip su Redis, una attesa di conferma su
     * RabbitMQ), n = messaggi, m = middleware del producer.
     */
    #[Override]
    public function publishBatch(iterable $messages): array
    {
        $envelopes = [];
        foreach ($messages as $message) {
            $envelope = $this->prepare($message)[1];
            if ($envelope instanceof Envelope) {
                $envelopes[] = $envelope;
            }
        }

        if ([] !== $envelopes) {
            $this->transport->sendBatch($envelopes);
        }

        return array_map(static fn (Envelope $envelope): MessageId => $envelope->message->id, $envelopes);
    }

    /**
     * @throws InvalidArgumentException
     *
     * @return array{MessageId, ?Envelope} Envelope null se un middleware ha scartato il messaggio
     */
    private function prepare(OutgoingMessage $outgoingMessage): array
    {
        $this->assertTextSize($outgoingMessage->text);

        $now = $this->clock->now();
        $message = new Message(
            $this->idGenerator->generate(),
            $outgoingMessage->channel,
            $outgoingMessage->text,
            $now,
            0,
            null === $outgoingMessage->ttlMs ? null : $now->modify(\sprintf('+%d milliseconds', $outgoingMessage->ttlMs)),
            $outgoingMessage->headers,
        );

        $prepared = null;
        $next = function (Message $message) use (&$prepared): void {
            $this->assertHeadersSize($message->headers);
            $prepared = $message;
        };

        foreach (array_reverse($this->middlewares) as $middleware) {
            $next = static function (Message $message) use ($middleware, $next): void {
                $middleware->process($message, $next);
            };
        }

        $next($message);

        return [$message->id, $prepared instanceof Message ? new Envelope($prepared, $outgoingMessage->delayMs) : null];
    }

    private function assertTextSize(string $text): void
    {
        if (\strlen($text) > $this->config->maxTextBytes) {
            throw new InvalidArgumentException(\sprintf('Text is %d bytes, limit is %d.', \strlen($text), $this->config->maxTextBytes));
        }
    }

    /**
     * @param array<string, string> $headers
     */
    private function assertHeadersSize(array $headers): void
    {
        $bytes = 0;
        foreach ($headers as $name => $value) {
            $bytes += \strlen($name) + \strlen($value);
        }

        if ($bytes > $this->config->maxHeadersBytes) {
            throw new InvalidArgumentException(\sprintf('Headers are %d bytes, limit is %d.', $bytes, $this->config->maxHeadersBytes));
        }
    }
}

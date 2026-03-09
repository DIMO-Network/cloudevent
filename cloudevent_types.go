// Package types provides event type constants for CloudEvents.
package cloudevent

const (
	// TypeStatus is the event type for status updates.
	TypeStatus = "dimo.status"

	// TypeStatus is the event type for status updates.
	TypeRawStatus = "dimo.raw.status"

	// TypeFingerprint is the event type for fingerprint updates.
	TypeFingerprint = "dimo.fingerprint"

	// TypeVerifableCredential is the event type for verifiable credentials.
	TypeVerifableCredential = "dimo.verifiablecredential" //nolint:gosec // This is not a credential.

	// TypeAttestation is the event type for 3rd party attestations
	TypeAttestation = "dimo.attestation"

	// TypeUnknown is the event type for unknown events.
	TypeUnknown = "dimo.unknown"

	// TypeEvent is the event type for vehicle events
	TypeSignals = "dimo.signals"

	// TypeEvent is the event type for vehicle events
	TypeEvent = "dimo.events"

	// TypeTrigger is the event type from a vehicle trigger.
	TypeTrigger = "dimo.trigger"

	// TypeSACD is the event type for SACD events.
	TypeSACD = "dimo.sacd"

	// TypeSACDTemplate is the event type for SACD template events.
	TypeSACDTemplate = "dimo.sacd.template"
)
